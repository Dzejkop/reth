//! This contains the [`BenchContext`], which is information that all replay-based benchmarks need.
//! The initialization code is also the same, so this can be shared across benchmark commands.

use crate::{authenticated_transport::AuthenticatedTransportConnect, bench_mode::BenchMode};
use alloy_eips::{eip7685::Requests, BlockNumberOrTag};
use alloy_primitives::{address, B256};
use alloy_provider::{network::AnyNetwork, Provider, RootProvider};
use alloy_rpc_client::ClientBuilder;
use alloy_rpc_types_engine::JwtSecret;
use alloy_transport::layers::RetryBackoffLayer;
use eyre::{Context, OptionExt};
use reqwest::Url;
use reth_node_core::args::BenchmarkArgs;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tracing::{info, warn};

pub(crate) use super::beacon_client::BeaconClient;

/// This is intended to be used by benchmarks that replay blocks from an RPC.
///
/// It contains an authenticated provider for engine API queries, a block provider for block
/// queries, a [`BenchMode`] to determine whether the benchmark should run for a closed or open
/// range of blocks, and the next block to fetch.
pub(crate) struct BenchContext {
    /// The auth provider is used for engine API queries.
    pub(crate) auth_provider: RootProvider<AnyNetwork>,
    /// The block provider is used for block queries.
    pub(crate) block_provider: RootProvider<AnyNetwork>,
    /// The benchmark mode, which defines whether the benchmark should run for a closed or open
    /// range of blocks.
    pub(crate) benchmark_mode: BenchMode,
    /// The next block to fetch.
    pub(crate) next_block: u64,
    /// Whether the chain is an OP rollup.
    pub(crate) is_optimism: bool,
    /// Optional beacon client for fetching execution requests.
    pub(crate) beacon_client: Option<Arc<BeaconClient>>,
}

impl BenchContext {
    /// This is the initialization code for most benchmarks, taking in a [`BenchmarkArgs`] and
    /// returning the providers needed to run a benchmark.
    pub(crate) async fn new(bench_args: &BenchmarkArgs, rpc_url: String) -> eyre::Result<Self> {
        info!("Running benchmark using data from RPC URL: {}", rpc_url);

        // Ensure that output directory exists and is a directory
        if let Some(output) = &bench_args.output {
            if output.is_file() {
                return Err(eyre::eyre!("Output path must be a directory"));
            }
            // Create the directory if it doesn't exist
            if !output.exists() {
                std::fs::create_dir_all(output)?;
                info!("Created output directory: {:?}", output);
            }
        }

        // set up alloy client for blocks
        let client = ClientBuilder::default()
            .layer(RetryBackoffLayer::new(10, 800, u64::MAX))
            .http(rpc_url.parse()?);
        let block_provider = RootProvider::<AnyNetwork>::new(client);

        // Check if this is an OP chain by checking code at a predeploy address.
        let is_optimism = !block_provider
            .get_code_at(address!("0x420000000000000000000000000000000000000F"))
            .await?
            .is_empty();

        // construct the authenticated provider
        let auth_jwt = bench_args
            .auth_jwtsecret
            .clone()
            .ok_or_else(|| eyre::eyre!("--jwt-secret must be provided for authenticated RPC"))?;

        // fetch jwt from file
        //
        // the jwt is hex encoded so we will decode it after
        let jwt = std::fs::read_to_string(auth_jwt)?;
        let jwt = JwtSecret::from_hex(jwt)?;

        // get engine url
        let auth_url = Url::parse(&bench_args.engine_rpc_url)?;

        // construct the authed transport
        info!("Connecting to Engine RPC at {} for replay", auth_url);
        let auth_transport = AuthenticatedTransportConnect::new(auth_url, jwt);
        let client = ClientBuilder::default().connect_with(auth_transport).await?;
        let auth_provider = RootProvider::<AnyNetwork>::new(client);

        // Computes the block range for the benchmark.
        //
        // - If `--advance` is provided, fetches the latest block and sets:
        //     - `from = head + 1`
        //     - `to = head + advance`
        // - Otherwise, uses the values from `--from` and `--to`.
        let (from, to) = if let Some(advance) = bench_args.advance {
            if advance == 0 {
                return Err(eyre::eyre!("--advance must be greater than 0"));
            }

            let head_block = auth_provider
                .get_block_by_number(BlockNumberOrTag::Latest)
                .await?
                .ok_or_else(|| eyre::eyre!("Failed to fetch latest block for --advance"))?;
            let head_number = head_block.header.number;
            (Some(head_number), Some(head_number + advance))
        } else {
            (bench_args.from, bench_args.to)
        };

        // If `--to` are not provided, we will run the benchmark continuously,
        // starting at the latest block.
        let latest_block = block_provider
            .get_block_by_number(BlockNumberOrTag::Latest)
            .full()
            .await?
            .ok_or_else(|| eyre::eyre!("Failed to fetch latest block from RPC"))?;
        let mut benchmark_mode = BenchMode::new(from, to, latest_block.into_inner().number())?;

        let first_block = match benchmark_mode {
            BenchMode::Continuous(start) => {
                block_provider.get_block_by_number(start.into()).full().await?.ok_or_else(|| {
                    eyre::eyre!("Failed to fetch block {} from RPC for continuous mode", start)
                })?
            }
            BenchMode::Range(ref mut range) => {
                match range.next() {
                    Some(block_number) => {
                        // fetch first block in range
                        block_provider
                            .get_block_by_number(block_number.into())
                            .full()
                            .await?
                            .ok_or_else(|| {
                                eyre::eyre!("Failed to fetch block {} from RPC", block_number)
                            })?
                    }
                    None => {
                        return Err(eyre::eyre!(
                            "Benchmark mode range is empty, please provide a larger range"
                        ));
                    }
                }
            }
        };

        let next_block = first_block.header.number + 1;

        // Initialize beacon client if URL is provided
        let beacon_client = match &bench_args.beacon_api_url {
            Some(url) => {
                let beacon_url = Url::parse(url)?;
                info!("Using Beacon API at {} for fetching execution requests", beacon_url);
                Some(Arc::new(BeaconClient::new(beacon_url).await?))
            }
            None => None,
        };

        Ok(Self {
            auth_provider,
            block_provider,
            benchmark_mode,
            next_block,
            is_optimism,
            beacon_client,
        })
    }
}

/// Block data fetched for benchmarking, including forkchoice state hashes.
pub(crate) struct BlockData {
    pub(crate) block: alloy_provider::network::AnyRpcBlock,
    pub(crate) head_block_hash: B256,
    pub(crate) safe_block_hash: B256,
    pub(crate) finalized_block_hash: B256,
    pub(crate) execution_requests: Option<Requests>,
}

/// Fetches blocks from RPC and sends them through the channel.
///
/// For each block, also fetches approximate safe (head - 32) and finalized (head - 64) block
/// hashes for forkchoice state construction.
pub(crate) async fn fetch_blocks(
    block_provider: RootProvider<AnyNetwork>,
    benchmark_mode: BenchMode,
    mut next_block: u64,
    sender: mpsc::Sender<BlockData>,
    error_sender: oneshot::Sender<eyre::Report>,
    beacon_client: Option<Arc<BeaconClient>>,
) {
    while benchmark_mode.contains(next_block) {
        let block_res = block_provider
            .get_block_by_number(next_block.into())
            .full()
            .await
            .wrap_err_with(|| format!("Failed to fetch block by number {next_block}"));
        let block = match block_res.and_then(|opt| opt.ok_or_eyre("Block not found")) {
            Ok(block) => block,
            Err(e) => {
                tracing::error!("Failed to fetch block {next_block}: {e}");
                let _ = error_sender.send(e);
                break;
            }
        };

        let head_block_hash = block.header.hash;
        let safe_block_hash =
            block_provider.get_block_by_number(block.header.number.saturating_sub(32).into());

        let finalized_block_hash =
            block_provider.get_block_by_number(block.header.number.saturating_sub(64).into());

        let (safe, finalized) = tokio::join!(safe_block_hash, finalized_block_hash);

        let safe_block_hash = match safe {
            Ok(Some(block)) => block.header.hash,
            Ok(None) | Err(_) => head_block_hash,
        };

        let finalized_block_hash = match finalized {
            Ok(Some(block)) => block.header.hash,
            Ok(None) | Err(_) => head_block_hash,
        };

        // Fetch execution requests from beacon API if available
        let execution_requests = if let Some(ref beacon) = beacon_client {
            match beacon.get_execution_requests(block.header.number, block.header.timestamp).await {
                Ok(requests) => requests,
                Err(e) => {
                    warn!(
                        block_number = block.header.number,
                        "Failed to fetch execution requests: {e}"
                    );
                    None
                }
            }
        } else {
            None
        };

        next_block += 1;
        if let Err(e) = sender
            .send(BlockData {
                block,
                head_block_hash,
                safe_block_hash,
                finalized_block_hash,
                execution_requests,
            })
            .await
        {
            tracing::error!("Failed to send block data: {e}");
            break;
        }
    }
}
