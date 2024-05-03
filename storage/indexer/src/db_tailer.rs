// Copyright (c) Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{
    db_tailer_reader::IndexerTransactionEventReader,
    schema::{
        event_by_key::EventByKeySchema, event_by_version::EventByVersionSchema,
        indexer_metadata::TailerMetadataSchema, transaction_by_account::TransactionByAccountSchema,
    },
    utils::{
        error_if_too_many_requested, get_first_seq_num_and_limit, AccountTransactionVersionIter,
        MAX_REQUEST_LIMIT,
    },
};
use aptos_config::config::index_db_tailer_config::IndexDBTailerConfig;
use aptos_schemadb::{ReadOptions, SchemaBatch, DB};
use aptos_storage_interface::{
    db_ensure as ensure, db_other_bail as bail, AptosDbError, DbReader, Order, Result,
};
use aptos_types::{
    account_address::AccountAddress,
    contract_event::{ContractEvent, EventWithVersion},
    event::EventKey,
    transaction::{AccountTransactionsWithProof, Version},
};
use std::sync::Arc;

pub struct DBTailer {
    pub last_version: Version,
    pub db: Arc<DB>,
    pub main_db_reader: Arc<dyn DbReader>,
    batch_size: usize,
}

impl DBTailer {
    pub fn new(db: Arc<DB>, db_reader: Arc<dyn DbReader>, config: &IndexDBTailerConfig) -> Self {
        let last_version = Self::initialize(db.clone());
        Self {
            last_version,
            db,
            main_db_reader: db_reader,
            batch_size: config.batch_size,
        }
    }

    fn initialize(db: Arc<DB>) -> Version {
        // read the latest key from the db
        let mut rev_iter_res = db
            .rev_iter::<TailerMetadataSchema>(Default::default())
            .expect("Cannot create db tailer metadata iterator");
        rev_iter_res
            .next()
            .map(|res| res.map_or(0, |(version, _)| version))
            .unwrap_or_default()
    }

    pub fn process_a_batch(&self, start_version: Option<Version>) -> Result<Version> {
        let db_iter = self
            .main_db_reader
            .get_db_backup_iter(start_version.unwrap_or(self.last_version), self.batch_size)
            .expect("Cannot create db tailer iterator");
        let batch = SchemaBatch::new();
        let metadata_batch = SchemaBatch::new();
        let mut version = self.last_version;
        db_iter.for_each(|res| {
            res.map(|(txn, events)| {
                if let Some(txn) = txn.try_as_signed_user_txn() {
                    batch
                        .put::<TransactionByAccountSchema>(
                            &(txn.sender(), txn.sequence_number()),
                            &version,
                        )
                        .expect("Failed to put txn to db tailer batch");

                    events.iter().enumerate().for_each(|(idx, event)| {
                        if let ContractEvent::V1(v1) = event {
                            batch
                                .put::<EventByKeySchema>(
                                    &(*v1.key(), v1.sequence_number()),
                                    &(version, idx as u64),
                                )
                                .expect("Failed to event by key to db tailer batch");
                            batch
                                .put::<EventByVersionSchema>(
                                    &(*v1.key(), version, v1.sequence_number()),
                                    &(idx as u64),
                                )
                                .expect("Failed to event by version to db tailer batch");
                        }
                    });
                }
                version += 1;
            })
            .expect("Failed to iterate db tailer iterator");
        });
        // write to index db
        self.db.write_schemas(batch)?;
        // update the metadata
        metadata_batch.put::<TailerMetadataSchema>(&version, &())?;
        self.db.write_schemas(metadata_batch)?;
        Ok(version)
    }

    pub fn get_last_version(&self) -> Version {
        self.last_version
    }

    pub fn get_account_transaction_version_iter(
        &self,
        address: AccountAddress,
        min_seq_num: u64,
        num_versions: u64,
        ledger_version: Version,
    ) -> Result<AccountTransactionVersionIter> {
        let mut iter = self
            .db
            .iter::<TransactionByAccountSchema>(ReadOptions::default())?;
        iter.seek(&(address, min_seq_num))?;
        Ok(AccountTransactionVersionIter::new(
            iter,
            address,
            min_seq_num
                .checked_add(num_versions)
                .ok_or(AptosDbError::TooManyRequested(min_seq_num, num_versions))?,
            ledger_version,
        ))
    }

    pub fn get_latest_sequence_number(
        &self,
        ledger_version: Version,
        event_key: &EventKey,
    ) -> Result<Option<u64>> {
        let mut iter = self
            .db
            .iter::<EventByVersionSchema>(ReadOptions::default())?;
        iter.seek_for_prev(&(*event_key, ledger_version, u64::max_value()))?;

        Ok(iter.next().transpose()?.and_then(
            |((key, _version, seq), _idx)| if &key == event_key { Some(seq) } else { None },
        ))
    }

    /// Given `event_key` and `start_seq_num`, returns events identified by transaction version and
    /// index among all events emitted by the same transaction. Result won't contain records with a
    /// transaction version > `ledger_version` and is in ascending order.
    pub fn lookup_events_by_key(
        &self,
        event_key: &EventKey,
        start_seq_num: u64,
        limit: u64,
        ledger_version: u64,
    ) -> Result<
        Vec<(
            u64,     // sequence number
            Version, // transaction version it belongs to
            u64,     // index among events for the same transaction
        )>,
    > {
        let mut iter = self.db.iter::<EventByKeySchema>(ReadOptions::default())?;
        iter.seek(&(*event_key, start_seq_num))?;

        let mut result = Vec::new();
        let mut cur_seq = start_seq_num;
        for res in iter.take(limit as usize) {
            let ((path, seq), (ver, idx)) = res?;
            if path != *event_key || ver > ledger_version {
                break;
            }
            if seq != cur_seq {
                let msg = if cur_seq == start_seq_num {
                    "First requested event is probably pruned."
                } else {
                    "DB corruption: Sequence number not continuous."
                };
                bail!("{} expected: {}, actual: {}", msg, cur_seq, seq);
            }
            result.push((seq, ver, idx));
            cur_seq += 1;
        }

        Ok(result)
    }
}

impl IndexerTransactionEventReader for DBTailer {
    fn get_events(
        &self,
        event_key: &EventKey,
        start: u64,
        order: Order,
        limit: u64,
        ledger_version: Version,
    ) -> Result<Vec<EventWithVersion>> {
        self.get_events_by_event_key(event_key, start, order, limit, ledger_version)
    }

    fn get_events_by_event_key(
        &self,
        event_key: &EventKey,
        start_seq_num: u64,
        order: Order,
        limit: u64,
        ledger_version: Version,
    ) -> Result<Vec<EventWithVersion>> {
        error_if_too_many_requested(limit, MAX_REQUEST_LIMIT)?;
        let get_latest = order == Order::Descending && start_seq_num == u64::max_value();

        let cursor = if get_latest {
            // Caller wants the latest, figure out the latest seq_num.
            // In the case of no events on that path, use 0 and expect empty result below.
            self.get_latest_sequence_number(ledger_version, event_key)?
                .unwrap_or(0)
        } else {
            start_seq_num
        };

        // Convert requested range and order to a range in ascending order.
        let (first_seq, real_limit) = get_first_seq_num_and_limit(order, cursor, limit)?;

        // Query the index.
        let mut event_indices =
            self.lookup_events_by_key(event_key, first_seq, real_limit, ledger_version)?;

        // When descending, it's possible that user is asking for something beyond the latest
        // sequence number, in which case we will consider it a bad request and return an empty
        // list.
        // For example, if the latest sequence number is 100, and the caller is asking for 110 to
        // 90, we will get 90 to 100 from the index lookup above. Seeing that the last item
        // is 100 instead of 110 tells us 110 is out of bound.
        if order == Order::Descending {
            if let Some((seq_num, _, _)) = event_indices.last() {
                if *seq_num < cursor {
                    event_indices = Vec::new();
                }
            }
        }

        let mut events_with_version = event_indices
            .into_iter()
            .map(|(seq, ver, idx)| {
                let event = self
                    .main_db_reader
                    .get_event_by_version_and_index(ver, idx)?;
                let v0 = match &event {
                    ContractEvent::V1(event) => event,
                    ContractEvent::V2(_) => bail!("Unexpected module event"),
                };
                ensure!(
                    seq == v0.sequence_number(),
                    "Index broken, expected seq:{}, actual:{}",
                    seq,
                    v0.sequence_number()
                );
                Ok(EventWithVersion::new(ver, event))
            })
            .collect::<Result<Vec<_>>>()?;
        if order == Order::Descending {
            events_with_version.reverse();
        }

        Ok(events_with_version)
    }

    fn get_account_transactions(
        &self,
        address: AccountAddress,
        start_seq_num: u64,
        limit: u64,
        include_events: bool,
        ledger_version: Version,
    ) -> Result<AccountTransactionsWithProof> {
        error_if_too_many_requested(limit, MAX_REQUEST_LIMIT)?;

        let txns_with_proofs = self
            .get_account_transaction_version_iter(address, start_seq_num, limit, ledger_version)?
            .map(|result| {
                let (_seq_num, txn_version) = result?;
                self.main_db_reader.get_transaction_with_proof(
                    txn_version,
                    ledger_version,
                    include_events,
                )
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(AccountTransactionsWithProof::new(txns_with_proofs))
    }
}
