//! Beacon API client for fetching execution requests.

use alloy_eips::{
    eip6110::{DepositRequest, DEPOSIT_REQUEST_TYPE},
    eip7002::{WithdrawalRequest, WITHDRAWAL_REQUEST_TYPE},
    eip7251::{ConsolidationRequest, CONSOLIDATION_REQUEST_TYPE},
    eip7685::Requests,
};
use reqwest::Url;
use serde::Deserialize;
use ssz::Encode;
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
    #[serde(default)]
    deposits: Vec<DepositRequest>,
    #[serde(default)]
    withdrawals: Vec<WithdrawalRequest>,
    #[serde(default)]
    consolidations: Vec<ConsolidationRequest>,
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
    pub(crate) async fn new(mut base_url: Url) -> eyre::Result<Self> {
        let client = reqwest::Client::new();

        // Ensure base URL ends with / for proper path joining
        if !base_url.path().ends_with('/') {
            base_url.set_path(&format!("{}/", base_url.path()));
        }

        // Fetch genesis time from beacon node
        let genesis_url = base_url.join("eth/v1/beacon/genesis")?;
        let response = client
            .get(genesis_url)
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
        let url = self.base_url.join(&format!("eth/v2/beacon/blocks/{}", slot))?;

        let response =
            self.client.get(url.clone()).header("Accept", "application/json").send().await?;

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

        // Build requests in EIP-7685 format: one entry per type containing
        // type byte + concatenated SSZ-encoded requests of that type.
        // Must be ordered by type and no duplicate types.
        let mut all_requests = Requests::default();

        if !execution_requests.deposits.is_empty() {
            let mut deposits_data = Vec::new();
            for deposit in &execution_requests.deposits {
                deposits_data.extend_from_slice(&deposit.as_ssz_bytes());
            }
            all_requests.push_request_with_type(DEPOSIT_REQUEST_TYPE, deposits_data);
        }

        if !execution_requests.withdrawals.is_empty() {
            let mut withdrawals_data = Vec::new();
            for withdrawal in &execution_requests.withdrawals {
                withdrawals_data.extend_from_slice(&withdrawal.as_ssz_bytes());
            }
            all_requests.push_request_with_type(WITHDRAWAL_REQUEST_TYPE, withdrawals_data);
        }

        if !execution_requests.consolidations.is_empty() {
            let mut consolidations_data = Vec::new();
            for consolidation in &execution_requests.consolidations {
                consolidations_data.extend_from_slice(&consolidation.as_ssz_bytes());
            }
            all_requests.push_request_with_type(CONSOLIDATION_REQUEST_TYPE, consolidations_data);
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
        // Block 22830127 details from mainnet (slot 12051519 from etherscan)
        let block_number = 22830127u64;
        let slot = 12051519u64;

        let beacon_url =
            std::env::var("BEACON_API_URL").unwrap_or_else(|_| "http://localhost:5052".to_string());

        println!("Fetching slot {} for block {} from {}", slot, block_number, beacon_url);

        let client = BeaconClient::new(beacon_url.parse().unwrap()).await.unwrap();

        // First fetch raw JSON to see actual structure
        let url = client.base_url.join(&format!("eth/v2/beacon/blocks/{}", slot)).unwrap();
        let raw: serde_json::Value = client
            .client
            .get(url)
            .header("Accept", "application/json")
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();

        if let Some(exec_req) = raw
            .get("data")
            .and_then(|d| d.get("message"))
            .and_then(|m| m.get("body"))
            .and_then(|b| b.get("execution_requests"))
        {
            println!("\n=== execution_requests structure ===");
            println!("{}", serde_json::to_string_pretty(exec_req).unwrap());
        } else {
            println!("No execution_requests field found");
        }

        let requests = client.try_fetch_requests(slot, block_number).await;
        println!("\nResult: {:?}", requests);

        if let Ok(Some(reqs)) = requests {
            println!("\nGot {} requests", reqs.len());
            for (i, req) in reqs.iter().enumerate() {
                if !req.is_empty() {
                    println!("  Request {}: type=0x{:02x}, len={}", i, req[0], req.len());
                }
            }
        }
    }
}
