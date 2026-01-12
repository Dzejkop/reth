//! Beacon API client for fetching execution requests.

use alloy_eips::eip7685::Requests;
use alloy_primitives::Bytes;
use reqwest::Url;
use serde::Deserialize;
use tracing::{info, warn};

/// Client for fetching data from the Beacon API.
#[derive(Debug, Clone)]
pub(crate) struct BeaconClient {
    /// The base URL of the beacon API.
    base_url: Url,
    /// HTTP client for making requests.
    client: reqwest::Client,
    /// Genesis time fetched from the beacon node.
    genesis_time: u64,
}

/// Slot duration in seconds (constant across all Ethereum networks currently).
const SECONDS_PER_SLOT: u64 = 12;

/// Maximum number of slots to search forward for missed slots.
const MAX_SLOT_SEARCH: u64 = 4;

/// Response wrapper for beacon API responses.
#[derive(Debug, Deserialize)]
struct BeaconResponse<T> {
    data: T,
}

/// Genesis data from the beacon API.
#[derive(Debug, Deserialize)]
struct GenesisData {
    #[serde(deserialize_with = "deserialize_u64_string")]
    genesis_time: u64,
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

/// Beacon block body containing execution payload and requests.
#[derive(Debug, Deserialize)]
struct BeaconBlockBody {
    /// Execution payload for block number verification.
    execution_payload: ExecutionPayload,
    /// Execution requests (only present in Electra+)
    #[serde(default)]
    execution_requests: Option<ExecutionRequestsResponse>,
}

/// Minimal execution payload for block number verification.
#[derive(Debug, Deserialize)]
struct ExecutionPayload {
    #[serde(deserialize_with = "deserialize_u64_string")]
    block_number: u64,
}

/// Execution requests as returned by the beacon API.
#[derive(Debug, Deserialize)]
struct ExecutionRequestsResponse {
    deposits: Vec<Bytes>,
    withdrawals: Vec<Bytes>,
    consolidations: Vec<Bytes>,
}

/// Deserialize a u64 from a quoted decimal string (beacon API format).
fn deserialize_u64_string<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s: String = serde::Deserialize::deserialize(deserializer)?;
    s.parse().map_err(serde::de::Error::custom)
}

impl BeaconClient {
    /// Creates a new beacon client, fetching genesis time from the beacon node.
    pub(crate) async fn new(base_url: Url) -> eyre::Result<Self> {
        let client = reqwest::Client::new();

        // Fetch genesis time from beacon node
        let genesis_url = format!("{}/eth/v1/beacon/genesis", base_url);
        let response = client
            .get(&genesis_url)
            .header("Accept", "application/json")
            .send()
            .await?
            .error_for_status()?;
        let genesis: BeaconResponse<GenesisData> = response.json().await?;

        info!(genesis_time = genesis.data.genesis_time, "Fetched beacon chain genesis time");

        Ok(Self { base_url, client, genesis_time: genesis.data.genesis_time })
    }

    /// Calculates the beacon slot for a given timestamp.
    const fn timestamp_to_slot(&self, timestamp: u64) -> u64 {
        timestamp.saturating_sub(self.genesis_time) / SECONDS_PER_SLOT
    }

    /// Fetches execution requests for a block, searching nearby slots if needed.
    ///
    /// Takes the block number and timestamp to calculate the slot and validate
    /// that the beacon block contains the correct execution block.
    pub(crate) async fn get_execution_requests(
        &self,
        block_number: u64,
        timestamp: u64,
    ) -> eyre::Result<Option<Requests>> {
        let base_slot = self.timestamp_to_slot(timestamp);

        // Search the calculated slot and a few after (for missed slots)
        for offset in 0..MAX_SLOT_SEARCH {
            let slot = base_slot + offset;
            match self.try_fetch_requests(slot, block_number).await {
                Ok(Some(requests)) => return Ok(Some(requests)),
                Ok(None) => {} // Slot empty or wrong block, try next
                Err(e) => {
                    warn!(slot, "Error fetching beacon block: {e}");
                }
            }
        }

        warn!(block_number, base_slot, "Could not find beacon block containing execution block");
        Ok(None)
    }

    /// Tries to fetch execution requests from a specific slot.
    ///
    /// Returns `Ok(Some(requests))` if the slot contains the expected block,
    /// `Ok(None)` if the slot is empty or contains a different block,
    /// or an error if the request failed.
    async fn try_fetch_requests(
        &self,
        slot: u64,
        expected_block_number: u64,
    ) -> eyre::Result<Option<Requests>> {
        let url = format!("{}/eth/v2/beacon/blocks/{}", self.base_url, slot);

        let response = self.client.get(&url).header("Accept", "application/json").send().await?;

        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }

        let response = response.error_for_status()?;
        let beacon_response: BeaconResponse<BeaconBlock> = response.json().await?;

        // Validate this beacon block contains our execution block
        let payload_block_number = beacon_response.data.message.body.execution_payload.block_number;
        if payload_block_number != expected_block_number {
            return Ok(None);
        }

        let Some(execution_requests) = beacon_response.data.message.body.execution_requests else {
            return Ok(None);
        };

        // Combine all requests into a single Requests object with type prefixes.
        // Beacon API returns requests without type prefixes (implicit from array),
        // but engine API expects each request prefixed with its type byte.
        let mut all_requests = Requests::default();
        for deposit in execution_requests.deposits {
            all_requests.push_request_with_type(0x00, deposit);
        }
        for withdrawal in execution_requests.withdrawals {
            all_requests.push_request_with_type(0x01, withdrawal);
        }
        for consolidation in execution_requests.consolidations {
            all_requests.push_request_with_type(0x02, consolidation);
        }

        Ok(Some(all_requests))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test fetching execution requests for block 22830127.
    /// Run with: cargo test -p reth-bench test_fetch_block_22830127 -- --nocapture --ignored
    #[tokio::test]
    #[ignore = "requires beacon API access"]
    async fn test_fetch_block_22830127() {
        // Block 22830127 details from mainnet
        let block_number = 22830127u64;
        let block_timestamp = 1736683583u64; // You may need to adjust this

        let beacon_url = std::env::var("BEACON_API_URL")
            .unwrap_or_else(|_| "http://localhost:5052".to_string());

        println!("Connecting to beacon API at: {}", beacon_url);

        let client = BeaconClient::new(beacon_url.parse().unwrap()).await.unwrap();

        println!("Genesis time: {}", client.genesis_time);

        let slot = client.timestamp_to_slot(block_timestamp);
        println!("Calculated slot for block {}: {}", block_number, slot);

        // Fetch raw beacon block to inspect
        let url = format!("{}/eth/v2/beacon/blocks/{}", client.base_url, slot);
        println!("Fetching: {}", url);

        let response = client
            .client
            .get(&url)
            .header("Accept", "application/json")
            .send()
            .await
            .unwrap();

        let raw_json: serde_json::Value = response.json().await.unwrap();

        // Print execution_requests from raw JSON
        if let Some(exec_requests) = raw_json
            .get("data")
            .and_then(|d| d.get("message"))
            .and_then(|m| m.get("body"))
            .and_then(|b| b.get("execution_requests"))
        {
            println!("\n=== Raw execution_requests from beacon API ===");
            println!("{}", serde_json::to_string_pretty(exec_requests).unwrap());

            if let Some(deposits) = exec_requests.get("deposits") {
                println!("\nDeposits count: {}", deposits.as_array().map(|a| a.len()).unwrap_or(0));
            }
            if let Some(withdrawals) = exec_requests.get("withdrawals") {
                println!("Withdrawals count: {}", withdrawals.as_array().map(|a| a.len()).unwrap_or(0));
            }
            if let Some(consolidations) = exec_requests.get("consolidations") {
                println!("Consolidations count: {}", consolidations.as_array().map(|a| a.len()).unwrap_or(0));
            }
        } else {
            println!("No execution_requests in beacon block");
        }

        // Now test our actual fetching logic
        println!("\n=== Testing get_execution_requests ===");
        let requests = client.get_execution_requests(block_number, block_timestamp).await.unwrap();

        match requests {
            Some(reqs) => {
                println!("Got {} requests", reqs.len());

                // Serialize to see what we'd send to engine API
                let serialized = serde_json::to_value(&reqs).unwrap();
                println!("\n=== Serialized Requests for engine API ===");
                println!("{}", serde_json::to_string_pretty(&serialized).unwrap());

                // Check each request's first byte (type)
                for (i, req) in reqs.iter().enumerate() {
                    if !req.is_empty() {
                        println!("Request {}: type=0x{:02x}, len={}", i, req[0], req.len());
                    } else {
                        println!("Request {}: EMPTY!", i);
                    }
                }
            }
            None => {
                println!("No execution requests found for block {}", block_number);
            }
        }
    }
}
