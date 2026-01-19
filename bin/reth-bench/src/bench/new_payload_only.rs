//! Runs the `reth bench` command, sending only newPayload, without a forkchoiceUpdated call.

use crate::{
    bench::{
        cached_payload::CachedPayloads,
        context::{fetch_blocks, BenchContext, BlockData},
        output::{
            NewPayloadResult, TotalGasOutput, TotalGasRow, GAS_OUTPUT_SUFFIX,
            NEW_PAYLOAD_OUTPUT_SUFFIX,
        },
    },
    valid_payload::{block_to_new_payload, call_new_payload},
};
use alloy_provider::network::AnyRpcBlock;
use clap::Parser;
use csv::Writer;
use reth_cli_runner::CliContext;
use reth_node_core::args::BenchmarkArgs;
use std::time::{Duration, Instant};
use tracing::{debug, info};

/// `reth benchmark new-payload-only` command
#[derive(Debug, Parser)]
pub struct Command {
    /// The RPC url to use for getting data.
    #[arg(long, value_name = "RPC_URL", verbatim_doc_comment)]
    rpc_url: Option<String>,

    /// The size of the block buffer (channel capacity) for prefetching blocks from the RPC
    /// endpoint.
    #[arg(
        long = "rpc-block-buffer-size",
        value_name = "RPC_BLOCK_BUFFER_SIZE",
        default_value = "20",
        verbatim_doc_comment
    )]
    rpc_block_buffer_size: usize,

    #[command(flatten)]
    benchmark: BenchmarkArgs,
}

impl Command {
    /// Execute `benchmark new-payload-only` command
    pub async fn execute(self, _ctx: CliContext) -> eyre::Result<()> {
        // Check that either rpc_url or payload_cache is provided
        if self.rpc_url.is_none() && self.benchmark.payload_cache.is_none() {
            eyre::bail!("Either --rpc-url or --payload-cache must be provided");
        }

        if let Some(cache_path) = self.benchmark.payload_cache.clone() {
            // Load from cache file
            self.execute_from_cache(&cache_path).await
        } else {
            // Fetch from RPC
            self.execute_from_rpc().await
        }
    }

    /// Execute benchmark using cached payloads from a file.
    async fn execute_from_cache(self, cache_path: &std::path::Path) -> eyre::Result<()> {
        info!(path = %cache_path.display(), "Loading payloads from cache");

        let cache = CachedPayloads::load(cache_path)?;
        info!(
            blocks = cache.blocks.len(),
            is_optimism = cache.is_optimism,
            "Loaded cached payloads"
        );

        // Use is_optimism from cache, only need auth provider for engine API calls
        let is_optimism = cache.is_optimism;
        let auth_provider = BenchContext::auth_provider_only(&self.benchmark).await?;

        let mut results = Vec::new();
        let total_benchmark_duration = Instant::now();

        for cached_block in cache.blocks {
            let block: AnyRpcBlock = serde_json::from_value(cached_block.block)?;
            let gas_used = block.header.gas_used;
            let block_number = block.header.number;
            let transaction_count = block.transactions.len() as u64;

            debug!(
                target: "reth-bench",
                number=?block_number,
                "Sending payload from cache to engine",
            );

            let (version, params) =
                block_to_new_payload(block, is_optimism, Some(cached_block.execution_requests))?;

            let start = Instant::now();
            call_new_payload(&auth_provider, version, params).await?;

            let new_payload_result = NewPayloadResult { gas_used, latency: start.elapsed() };
            info!(%new_payload_result);

            let current_duration = total_benchmark_duration.elapsed();

            let row =
                TotalGasRow { block_number, transaction_count, gas_used, time: current_duration };
            results.push((row, new_payload_result));
        }

        self.write_results(results)
    }

    /// Execute benchmark fetching blocks from RPC.
    async fn execute_from_rpc(self) -> eyre::Result<()> {
        let rpc_url = self
            .rpc_url
            .clone()
            .ok_or_else(|| eyre::eyre!("--rpc-url is required when not using --payload-cache"))?;

        let BenchContext { benchmark_mode, block_provider, auth_provider, next_block, is_optimism } =
            BenchContext::new(&self.benchmark, rpc_url).await?;

        let buffer_size = self.rpc_block_buffer_size;

        let (error_sender, mut error_receiver) = tokio::sync::oneshot::channel();
        let (sender, mut receiver) = tokio::sync::mpsc::channel::<BlockData>(buffer_size);

        tokio::task::spawn(fetch_blocks(
            block_provider,
            benchmark_mode,
            next_block,
            sender,
            error_sender,
        ));

        let mut results = Vec::new();
        let total_benchmark_duration = Instant::now();
        let mut total_wait_time = Duration::ZERO;

        while let Some(BlockData { block, execution_requests, .. }) = {
            let wait_start = Instant::now();
            let result = receiver.recv().await;
            total_wait_time += wait_start.elapsed();
            result
        } {
            let block_number = block.header.number;
            let transaction_count = block.transactions.len() as u64;
            let gas_used = block.header.gas_used;

            debug!(
                target: "reth-bench",
                number=?block.header.number,
                "Sending payload to engine",
            );

            let (version, params) = block_to_new_payload(block, is_optimism, execution_requests)?;

            let start = Instant::now();
            call_new_payload(&auth_provider, version, params).await?;

            let new_payload_result = NewPayloadResult { gas_used, latency: start.elapsed() };
            info!(%new_payload_result);

            let current_duration = total_benchmark_duration.elapsed() - total_wait_time;

            let row =
                TotalGasRow { block_number, transaction_count, gas_used, time: current_duration };
            results.push((row, new_payload_result));
        }

        if let Ok(error) = error_receiver.try_recv() {
            return Err(error);
        }

        self.write_results(results)
    }

    /// Write benchmark results to files.
    fn write_results(&self, results: Vec<(TotalGasRow, NewPayloadResult)>) -> eyre::Result<()> {
        let (gas_output_results, new_payload_results): (_, Vec<NewPayloadResult>) =
            results.into_iter().unzip();

        if let Some(path) = &self.benchmark.output {
            let output_path = path.join(NEW_PAYLOAD_OUTPUT_SUFFIX);
            info!("Writing newPayload call latency output to file: {:?}", output_path);
            let mut writer = Writer::from_path(output_path)?;
            for result in new_payload_results {
                writer.serialize(result)?;
            }
            writer.flush()?;

            let output_path = path.join(GAS_OUTPUT_SUFFIX);
            info!("Writing total gas output to file: {:?}", output_path);
            let mut writer = Writer::from_path(output_path)?;
            for row in &gas_output_results {
                writer.serialize(row)?;
            }
            writer.flush()?;

            info!("Finished writing benchmark output files to {:?}.", path);
        }

        let gas_output = TotalGasOutput::new(gas_output_results)?;
        info!(
            total_duration=?gas_output.total_duration,
            total_gas_used=?gas_output.total_gas_used,
            blocks_processed=?gas_output.blocks_processed,
            "Total Ggas/s: {:.4}",
            gas_output.total_gigagas_per_second()
        );

        Ok(())
    }
}
