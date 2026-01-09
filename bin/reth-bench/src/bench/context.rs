//! This contains the [`BenchContext`], which is information that all replay-based benchmarks need.
//! The initialization code is also the same, so this can be shared across benchmark commands.

use crate::{authenticated_transport::AuthenticatedTransportConnect, bench_mode::BenchMode};
use alloy_eips::{eip7685::Requests, BlockNumberOrTag};
use alloy_primitives::{address, Bytes, B256};
use alloy_provider::{network::AnyNetwork, Provider, RootProvider};
use alloy_rpc_client::ClientBuilder;
use alloy_rpc_types_engine::JwtSecret;
use alloy_transport::layers::RetryBackoffLayer;
use eyre::{Context, OptionExt};
use reqwest::Url;
use reth_node_core::args::BenchmarkArgs;
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tracing::{info, warn};

/// Client for fetching data from the Beacon API.
#[derive(Debug, Clone)]
pub(crate) struct BeaconClient {
    /// The base URL of the beacon API.
    base_url: Url,
    /// HTTP client for making requests.
    client: reqwest::Client,
}

/// Response wrapper for beacon API responses.
#[derive(Debug, Deserialize)]
struct BeaconResponse<T> {
    data: T,
}

/// Beacon block response from the API.
#[derive(Debug, Deserialize)]
struct BeaconBlock {
    message: BeaconBlockMessage,
}

/// Beacon block message containing the body.
#[derive(Debug, Deserialize)]
struct BeaconBlockMessage {
    body: BeaconBlockBody,
}

/// Beacon block body containing execution requests.
#[derive(Debug, Deserialize)]
struct BeaconBlockBody {
    /// Execution requests (only present in Electra+)
    #[serde(default)]
    execution_requests: Option<ExecutionRequestsResponse>,
}

/// Execution requests as returned by the beacon API.
#[derive(Debug, Deserialize)]
struct ExecutionRequestsResponse {
    deposits: Vec<Bytes>,
    withdrawals: Vec<Bytes>,
    consolidations: Vec<Bytes>,
}

impl BeaconClient {
    /// Creates a new beacon client with the given base URL.
    pub(crate) fn new(base_url: Url) -> Self {
        Self { base_url, client: reqwest::Client::new() }
    }

    /// Fetches execution requests for a given slot from the beacon API.
    pub(crate) async fn get_execution_requests(&self, slot: u64) -> eyre::Result<Option<Requests>> {
        let url = format!("{}/eth/v2/beacon/blocks/{}", self.base_url, slot);

        let response = self.client.get(&url).header("Accept", "application/json").send().await?;

        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }

        let response = response.error_for_status()?;
        let beacon_response: BeaconResponse<BeaconBlock> = response.json().await?;

        let Some(execution_requests) = beacon_response.data.message.body.execution_requests else {
            return Ok(None);
        };

        // Combine all requests into a single Requests object
        // Each request type is prefixed with its type byte (0x00 for deposits, 0x01 for
        // withdrawals, 0x02 for consolidations)
        let mut all_requests = Vec::new();

        for deposit in execution_requests.deposits {
            all_requests.push(deposit);
        }
        for withdrawal in execution_requests.withdrawals {
            all_requests.push(withdrawal);
        }
        for consolidation in execution_requests.consolidations {
            all_requests.push(consolidation);
        }

        Ok(Some(all_requests.into()))
    }
}

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
        let beacon_client = bench_args.beacon_api_url.as_ref().map(|url| {
            let beacon_url = Url::parse(url).expect("Invalid beacon API URL");
            info!("Using Beacon API at {} for fetching execution requests", beacon_url);
            Arc::new(BeaconClient::new(beacon_url))
        });

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
pub(crate) type BlockData = (
    alloy_provider::network::AnyRpcBlock,
    B256,             // head_block_hash
    B256,             // safe_block_hash
    B256,             // finalized_block_hash
    Option<Requests>, // execution_requests
);

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
            // Convert block timestamp to slot number
            // Slot = (timestamp - genesis_time) / 12
            // For mainnet, genesis_time is 1606824023 (Dec 1, 2020)
            const GENESIS_TIME: u64 = 1606824023;
            const SLOT_DURATION: u64 = 12;

            let slot = (block.header.timestamp.saturating_sub(GENESIS_TIME)) / SLOT_DURATION;

            match beacon.get_execution_requests(slot).await {
                Ok(requests) => requests,
                Err(e) => {
                    warn!("Failed to fetch execution requests for slot {slot}: {e}");
                    None
                }
            }
        } else {
            None
        };

        next_block += 1;
        if let Err(e) = sender
            .send((
                block,
                head_block_hash,
                safe_block_hash,
                finalized_block_hash,
                execution_requests,
            ))
            .await
        {
            tracing::error!("Failed to send block data: {e}");
            break;
        }
    }
}
