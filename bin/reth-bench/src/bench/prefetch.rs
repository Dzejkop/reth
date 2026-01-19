//! Prefetch command for downloading blocks and execution requests to a cache file.
//!
//! This command fetches blocks from an RPC endpoint and execution requests from a beacon API,
//! saving them to a JSON file for offline benchmarking.

use crate::bench::{
    beacon_client::BeaconClient,
    cached_payload::{CachedBlockData, CachedPayloads},
};
use alloy_eips::eip7685::Requests;
use alloy_primitives::address;
use alloy_provider::{network::AnyNetwork, Provider, RootProvider};
use alloy_rpc_client::ClientBuilder;
use alloy_transport::layers::RetryBackoffLayer;
use clap::Parser;
use reqwest::Url;
use reth_cli_runner::CliContext;
use std::path::PathBuf;
use tracing::{info, warn};

/// `reth benchmark prefetch` command
///
/// Pre-fetches blocks and execution requests to a JSON file for offline benchmarking.
#[derive(Debug, Parser)]
pub struct Command {
    /// The RPC url to use for fetching blocks.
    #[arg(long, value_name = "RPC_URL", verbatim_doc_comment)]
    rpc_url: String,

    /// Beacon API URL for fetching execution requests.
    #[arg(long, value_name = "BEACON_API_URL", verbatim_doc_comment)]
    beacon_api_url: String,

    /// Starting block number (inclusive).
    #[arg(long)]
    from: u64,

    /// Ending block number (inclusive).
    #[arg(long)]
    to: u64,

    /// Output file path for the cached payloads.
    #[arg(long, short)]
    output: PathBuf,
}

impl Command {
    /// Execute `benchmark prefetch` command
    pub async fn execute(self, _ctx: CliContext) -> eyre::Result<()> {
        if self.from > self.to {
            eyre::bail!("--from ({}) must be <= --to ({})", self.from, self.to);
        }

        info!(
            from = self.from,
            to = self.to,
            output = %self.output.display(),
            "Pre-fetching blocks and execution requests"
        );

        // Set up block provider
        let client = ClientBuilder::default()
            .layer(RetryBackoffLayer::new(10, 800, u64::MAX))
            .http(self.rpc_url.parse()?);
        let block_provider = RootProvider::<AnyNetwork>::new(client);

        // Set up beacon client
        let beacon_url = Url::parse(&self.beacon_api_url)?;
        let beacon_client = BeaconClient::new(beacon_url).await?;

        // Detect Optimism by checking code at a predeploy address
        let is_optimism = !block_provider
            .get_code_at(address!("0x420000000000000000000000000000000000000F"))
            .await?
            .is_empty();
        if is_optimism {
            info!("Detected Optimism chain");
        }

        let mut cache = CachedPayloads::new(is_optimism);
        let total_blocks = self.to - self.from + 1;

        for block_number in self.from..=self.to {
            let progress = block_number - self.from + 1;

            // Fetch the block
            let block = block_provider
                .get_block_by_number(block_number.into())
                .full()
                .await?
                .ok_or_else(|| eyre::eyre!("Block {} not found", block_number))?;

            let head_block_hash = block.header.hash;
            let timestamp = block.header.timestamp;

            // Fetch safe and finalized block hashes
            let safe_block_hash = block_provider
                .get_block_by_number(block_number.saturating_sub(32).into())
                .await?
                .map(|b| b.header.hash)
                .unwrap_or(head_block_hash);

            let finalized_block_hash = block_provider
                .get_block_by_number(block_number.saturating_sub(64).into())
                .await?
                .map(|b| b.header.hash)
                .unwrap_or(head_block_hash);

            // Fetch execution requests from beacon API (empty for Optimism or pre-Prague)
            let execution_requests =
                match beacon_client.get_execution_requests(block_number, timestamp).await {
                    Ok(requests) => requests.unwrap_or_default(),
                    Err(e) => {
                        warn!(block_number, "Failed to fetch execution requests: {e}");
                        Requests::default()
                    }
                };

            // Serialize block to JSON for storage
            let block_json = serde_json::to_value(&block)?;

            cache.blocks.push(CachedBlockData {
                block: block_json,
                head_block_hash,
                safe_block_hash,
                finalized_block_hash,
                execution_requests,
            });

            if progress.is_multiple_of(10) || progress == total_blocks {
                info!(progress, total = total_blocks, block_number, "Fetched block");
            }
        }

        // Save to file
        cache.save(&self.output)?;
        info!(
            blocks = cache.blocks.len(),
            path = %self.output.display(),
            "Saved cached payloads"
        );

        Ok(())
    }
}
