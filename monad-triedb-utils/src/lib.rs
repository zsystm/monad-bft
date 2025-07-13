use std::{
    path::Path,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering::SeqCst},
        Arc,
    },
};

use alloy_consensus::Header;
use alloy_primitives::Address;
use alloy_rlp::Decodable;
use futures::{channel::oneshot, executor::block_on, future::join_all, FutureExt};
use key::Version;
use monad_bls::BlsSignatureCollection;
use monad_consensus_types::block::ConsensusBlockHeader;
use monad_crypto::{
    certificate_signature::CertificateSignaturePubKey,
    hasher::{Hasher, HasherType},
};
use monad_eth_types::{EthAccount, EthExecutionProtocol, EthHeader};
use monad_secp::SecpSignature;
use monad_state_backend::{StateBackend, StateBackendError};
use monad_triedb::TriedbHandle;
use monad_types::{BlockId, Hash, SeqNum, GENESIS_BLOCK_ID, GENESIS_SEQ_NUM};
use tracing::{debug, trace, warn};

use crate::{
    decode::rlp_decode_account,
    key::{create_triedb_key, KeyInput},
};

pub mod decode;
pub mod key;
pub mod mock_triedb;
pub mod triedb_env;

const MAX_TRIEDB_ASYNC_POLLS: usize = 640_000;

#[derive(Clone)]
pub struct TriedbReader {
    handle: TriedbHandle,
    state_backend_total_lookups: Arc<AtomicU64>,
}

impl TriedbReader {
    /// for debug only
    #[allow(clippy::missing_safety_doc)]
    pub unsafe fn handle(&self) -> &TriedbHandle {
        &self.handle
    }

    pub fn try_new(triedb_path: &Path) -> Option<Self> {
        TriedbHandle::try_new(triedb_path).map(|handle| Self {
            handle,
            state_backend_total_lookups: Default::default(),
        })
    }

    pub fn get_latest_voted_block(&self) -> Option<SeqNum> {
        let latest_voted = self.handle.latest_voted_block()?;
        Some(SeqNum(latest_voted))
    }

    pub fn get_latest_voted_block_id(&self) -> Option<BlockId> {
        let latest_voted = self.handle.latest_voted_block_id()?;
        Some(BlockId(Hash(latest_voted)))
    }

    pub fn get_latest_finalized_block(&self) -> Option<SeqNum> {
        let latest_finalized = self.handle.latest_finalized_block()?;
        Some(SeqNum(latest_finalized))
    }

    pub fn get_earliest_finalized_block(&self) -> Option<SeqNum> {
        let earliest_finalized = self.handle.earliest_finalized_block()?;
        Some(SeqNum(earliest_finalized))
    }

    pub fn get_account_finalized(
        &self,
        seq_num: &SeqNum,
        eth_address: &[u8; 20],
    ) -> Option<EthAccount> {
        self.get_account(seq_num, Version::Finalized, eth_address)
    }

    pub fn get_finalized_eth_header(&self, seq_num: &SeqNum) -> Option<EthHeader> {
        if self
            .raw_read_latest_finalized_block()
            .is_some_and(|latest_finalized| seq_num <= &latest_finalized)
        {
            let (triedb_key, key_len_nibbles) =
                create_triedb_key(Version::Finalized, KeyInput::BlockHeader);
            let eth_header_bytes = self.handle.read(&triedb_key, key_len_nibbles, seq_num.0)?;
            let mut rlp_buf = eth_header_bytes.as_slice();
            let block_header = Header::decode(&mut rlp_buf).expect("invalid rlp eth header");

            Some(EthHeader(block_header))
        } else {
            None
        }
    }

    pub fn get_proposed_eth_header(
        &self,
        block_id: &BlockId,
        seq_num: &SeqNum,
    ) -> Option<EthHeader> {
        let (triedb_key, key_len_nibbles) =
            create_triedb_key(Version::Proposal(*block_id), KeyInput::BlockHeader);
        let eth_header_bytes = self.handle.read(&triedb_key, key_len_nibbles, seq_num.0)?;

        let mut rlp_buf = eth_header_bytes.as_slice();
        let block_header = Header::decode(&mut rlp_buf).expect("invalid rlp eth header");

        Some(EthHeader(block_header))
    }

    // only guaranteed to work for proposals
    pub fn get_bft_block(
        &self,
        seq_num: &SeqNum,
        block_id: &BlockId,
    ) -> Option<
        ConsensusBlockHeader<
            SecpSignature,
            BlsSignatureCollection<CertificateSignaturePubKey<SecpSignature>>,
            EthExecutionProtocol,
        >,
    > {
        if seq_num == &GENESIS_SEQ_NUM || block_id == &GENESIS_BLOCK_ID {
            // execution populates a garbage genesis consensus block
            return None;
        }

        let (triedb_key, key_len_nibbles) =
            create_triedb_key(Version::Proposal(*block_id), KeyInput::BftBlock);
        let bft_block = self.handle.read(&triedb_key, key_len_nibbles, seq_num.0)?;

        let block: ConsensusBlockHeader<_, _, _> = alloy_rlp::decode_exact(&bft_block)
            .unwrap_or_else(|err| {
                panic!(
                    "failed to decode rlp ConsensusBlockHeader, err={:?}, seq_num={:?}, block_id={:?}, hex_hash={}, hex={}",
                    err,
                    seq_num,
                    block_id,
                    {
                        let mut hasher = HasherType::new();
                        hasher.update(&bft_block);
                        hex::encode(hasher.hash().0)
                    },
                    hex::encode(bft_block),
                )
            });

        Some(block)
    }

    // for accessing Version::Proposed, bft_id MUST BE VERIFIED
    pub fn get_account(
        &self,
        seq_num: &SeqNum,
        version: Version,
        eth_address: &[u8; 20],
    ) -> Option<EthAccount> {
        let (triedb_key, key_len_nibbles) =
            create_triedb_key(version, KeyInput::Address(eth_address));

        let result = self.handle.read(&triedb_key, key_len_nibbles, seq_num.0);

        let Some(account_rlp) = result else {
            debug!(?seq_num, ?eth_address, "account not found");
            return None;
        };

        rlp_decode_account(account_rlp)
    }

    pub fn get_accounts_async<'a>(
        &self,
        seq_num: &SeqNum,
        version: Version,
        eth_addresses: impl Iterator<Item = &'a Address>,
    ) -> Option<Vec<Option<EthAccount>>> {
        // Counter which is updated when TrieDB processes a single async read to completion
        let completed_counter = Arc::new(AtomicUsize::new(0));
        let mut num_accounts = 0;
        let eth_account_receivers = eth_addresses.map(|eth_address| {
            num_accounts += 1;
            let (triedb_key, key_len_nibbles) =
                create_triedb_key(version, KeyInput::Address(eth_address.as_ref()));
            let (sender, receiver) = oneshot::channel();
            self.handle.read_async(
                triedb_key.as_ref(),
                key_len_nibbles,
                seq_num.0,
                completed_counter.clone(),
                sender,
                Arc::new(()),
            );
            receiver.map(|receiver_result| {
                // Receiver should not fail
                let maybe_rlp_account = receiver_result.expect("receiver can't be canceled");
                // RLP decode the received account value
                maybe_rlp_account.and_then(rlp_decode_account)
            })
        });
        // Join all futures of receivers
        let eth_account_receivers = join_all(eth_account_receivers);
        let mut poll_count = 0;
        // Poll TrieDB until the completed_counter reaches num_accounts
        // TODO: if initiating the reads had an error, this call to triedb_poll will not terminate.
        //       wrap this loop in a timeout so consensus doesn't get stuck
        while completed_counter.load(SeqCst) < num_accounts && poll_count < MAX_TRIEDB_ASYNC_POLLS {
            // blocking = true => wait to do more work
            // max_completions = usize::MAX => process as many completions before returning
            poll_count += self.handle.triedb_poll(true, usize::MAX);
        }
        // TrieDB should have completed processing all the async callbacks at this point
        if completed_counter.load(SeqCst) != num_accounts {
            warn!(
                "TrieDB poll count went over limit for {} lookups",
                num_accounts
            );
            return None;
        }
        Some(block_on(eth_account_receivers))
    }
}

impl StateBackend for TriedbReader {
    fn get_account_statuses<'a>(
        &self,
        block_id: &BlockId,
        seq_num: &SeqNum,
        is_finalized: bool,
        eth_addresses: impl Iterator<Item = &'a Address>,
    ) -> Result<Vec<Option<EthAccount>>, StateBackendError> {
        let statuses = if is_finalized
            && self
                .raw_read_latest_finalized_block()
                .is_some_and(|latest_finalized| seq_num <= &latest_finalized)
        {
            trace!(?seq_num, "triedb read finalized");
            // check finalized

            // block <= latest
            let Some(statuses) =
                self.get_accounts_async(seq_num, Version::Finalized, eth_addresses)
            else {
                return Err(StateBackendError::NotAvailableYet);
            };

            let earliest = self
                .raw_read_earliest_finalized_block()
                .expect("earliest must exist if latest does");
            if seq_num < &earliest {
                // block < earliest
                return Err(StateBackendError::NeverAvailable);
            }
            // block >= earliest
            statuses
        } else {
            trace!(?seq_num, ?block_id, "triedb read proposed");

            // make sure the block is committed before reading accounts
            let Some(_header) = self.get_proposed_eth_header(block_id, seq_num) else {
                return Err(StateBackendError::NotAvailableYet);
            };

            let Some(statuses) =
                self.get_accounts_async(seq_num, Version::Proposal(*block_id), eth_addresses)
            else {
                return Err(StateBackendError::NotAvailableYet);
            };
            statuses
        };

        self.state_backend_total_lookups
            .fetch_add(statuses.len() as u64, std::sync::atomic::Ordering::SeqCst);

        Ok(statuses)
    }

    fn get_execution_result(
        &self,
        block_id: &BlockId,
        seq_num: &SeqNum,
        is_finalized: bool,
    ) -> Result<EthHeader, StateBackendError> {
        if is_finalized
            && self
                .raw_read_latest_finalized_block()
                .is_some_and(|latest_finalized| seq_num <= &latest_finalized)
        {
            trace!(?seq_num, "triedb read eth header finalized");
            // check finalized

            // block <= latest
            let Some(header) = self.get_finalized_eth_header(seq_num) else {
                return Err(StateBackendError::NotAvailableYet);
            };
            Ok(header)
        } else {
            // check proposed
            trace!(?seq_num, ?block_id, "triedb read eth header proposed");

            let Some(header) = self.get_proposed_eth_header(block_id, seq_num) else {
                return Err(StateBackendError::NotAvailableYet);
            };
            Ok(header)
        }
    }

    fn raw_read_earliest_finalized_block(&self) -> Option<SeqNum> {
        self.get_earliest_finalized_block()
    }

    fn raw_read_latest_finalized_block(&self) -> Option<SeqNum> {
        self.get_latest_finalized_block()
    }

    fn total_db_lookups(&self) -> u64 {
        self.state_backend_total_lookups
            .load(std::sync::atomic::Ordering::SeqCst)
    }
}
