//! Cached payload types for offline benchmarking.
//!
//! These types allow pre-fetching blocks and execution requests to a file,
//! which can then be used for benchmarking without live beacon API access.

use alloy_eips::eip7685::Requests;
use alloy_primitives::B256;
use serde::{Deserialize, Serialize};

/// Current version of the cache format.
pub(crate) const CACHE_VERSION: u32 = 1;

/// Cached data for a single block, including forkchoice state hashes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CachedBlockData {
    /// The RPC block data as JSON.
    pub(crate) block: serde_json::Value,
    /// Head block hash for forkchoice state.
    pub(crate) head_block_hash: B256,
    /// Safe block hash for forkchoice state.
    pub(crate) safe_block_hash: B256,
    /// Finalized block hash for forkchoice state.
    pub(crate) finalized_block_hash: B256,
    /// Execution requests from beacon API (if available).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) execution_requests: Option<Requests>,
}

/// Container for cached payloads with version information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CachedPayloads {
    /// Version of the cache format for forward compatibility.
    pub(crate) version: u32,
    /// Cached block data.
    pub(crate) blocks: Vec<CachedBlockData>,
}

impl CachedPayloads {
    /// Creates a new empty cache.
    pub(crate) const fn new() -> Self {
        Self { version: CACHE_VERSION, blocks: Vec::new() }
    }

    /// Loads cached payloads from a file.
    pub(crate) fn load(path: &std::path::Path) -> eyre::Result<Self> {
        let file = std::fs::File::open(path)?;
        let reader = std::io::BufReader::new(file);
        let cache: Self = serde_json::from_reader(reader)?;

        if cache.version != CACHE_VERSION {
            eyre::bail!(
                "Cache version mismatch: expected {}, got {}",
                CACHE_VERSION,
                cache.version
            );
        }

        Ok(cache)
    }

    /// Saves cached payloads to a file.
    pub(crate) fn save(&self, path: &std::path::Path) -> eyre::Result<()> {
        let file = std::fs::File::create(path)?;
        let writer = std::io::BufWriter::new(file);
        serde_json::to_writer_pretty(writer, self)?;
        Ok(())
    }
}
