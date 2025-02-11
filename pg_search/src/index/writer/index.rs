// Copyright (c) 2023-2025 Retake, Inc.
//
// This file is part of ParadeDB - Postgres for Search and Analytics
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use anyhow::Result;
use pgrx::PgRelation;
use std::collections::HashSet;
use std::sync::Arc;
use tantivy::index::SegmentId;
use tantivy::indexer::UserOperation;
use tantivy::schema::Field;
use tantivy::{DocId, Index, IndexSettings, IndexWriter, Opstamp, TantivyDocument, TantivyError};
use thiserror::Error;

use crate::index::channel::{ChannelDirectory, ChannelRequestHandler};
use crate::index::{get_index_schema, setup_tokenizers, BlockDirectoryType, WriterResources};
use crate::{
    postgres::types::TantivyValueError,
    schema::{SearchDocument, SearchIndexSchema},
};

// NB:  should this be a GUC?  Could be useful or could just complicate things for the user
/// How big should our insert queue get before we go ahead and add them to the tantivy index?
const MAX_INSERT_QUEUE_SIZE: usize = 1000;
const CHANNEL_QUEUE_LEN: usize = 1000;

/// The entity that interfaces with Tantivy indexes.
pub struct SearchIndexWriter {
    pub schema: SearchIndexSchema,
    ctid_field: Field,

    // keep all these private -- leaking them to the public API would allow callers to
    // mis-use the IndexWriter in particular.
    writer: Arc<IndexWriter>,
    handler: ChannelRequestHandler,
    insert_queue: Vec<UserOperation>,
}

impl SearchIndexWriter {
    pub fn open(
        index_relation: &PgRelation,
        directory_type: BlockDirectoryType,
        resources: WriterResources,
    ) -> Result<Self> {
        let (parallelism, memory_budget, wants_merge) = resources.resources();

        let (req_sender, req_receiver) = crossbeam::channel::bounded(CHANNEL_QUEUE_LEN);
        let channel_dir = ChannelDirectory::new(req_sender);
        let mut handler =
            directory_type.channel_request_handler(index_relation, req_receiver, wants_merge);

        let mut index = {
            handler
                .wait_for(move || {
                    let index = Index::open(channel_dir)?;
                    tantivy::Result::Ok(index)
                })
                .expect("scoped thread should not fail")?
        };
        setup_tokenizers(&mut index, index_relation);

        let index_clone = index.clone();
        let writer = handler
            .wait_for(move || {
                let writer =
                    index_clone.writer_with_num_threads(parallelism.get(), memory_budget)?;
                tantivy::Result::Ok(writer)
            })
            .expect("scoped thread should not fail")?;

        let schema = SearchIndexSchema::open(index.schema(), index_relation);
        let ctid_field = schema.schema.get_field("ctid")?;

        Ok(Self {
            writer: Arc::new(writer),
            schema,
            handler,
            ctid_field,
            insert_queue: Vec::with_capacity(MAX_INSERT_QUEUE_SIZE),
        })
    }

    pub fn create_index(index_relation: &PgRelation) -> Result<Self> {
        let schema = get_index_schema(index_relation)?;
        let (parallelism, memory_budget, merge_policy) = WriterResources::CreateIndex.resources();

        let (req_sender, req_receiver) = crossbeam::channel::bounded(CHANNEL_QUEUE_LEN);
        let channel_dir = ChannelDirectory::new(req_sender);
        let mut handler = BlockDirectoryType::Mvcc.channel_request_handler(
            index_relation,
            req_receiver,
            merge_policy,
        );

        let mut index = {
            let schema = schema.clone();
            let settings = IndexSettings {
                docstore_compress_dedicated_thread: false,
                ..IndexSettings::default()
            };

            handler
                .wait_for(move || {
                    let index = Index::create(channel_dir, schema.schema.clone(), settings)?;
                    tantivy::Result::Ok(index)
                })
                .expect("scoped thread should not fail")?
        };
        setup_tokenizers(&mut index, index_relation);

        let writer = handler
            .wait_for(move || {
                let writer = index.writer_with_num_threads(parallelism.get(), memory_budget)?;
                tantivy::Result::Ok(writer)
            })
            .expect("scoped thread should not fail")?;
        let ctid_field = schema.schema.get_field("ctid")?;

        Ok(Self {
            writer: Arc::new(writer),
            schema,
            ctid_field,
            handler,
            insert_queue: Vec::with_capacity(MAX_INSERT_QUEUE_SIZE),
        })
    }

    pub fn segment_ids(&mut self) -> HashSet<SegmentId> {
        let index = self.writer.index().clone();
        self.handler
            .wait_for(move || index.searchable_segment_ids().unwrap())
            .unwrap()
            .into_iter()
            .collect()
    }

    pub fn delete_document(&mut self, segment_id: SegmentId, doc_id: DocId) -> Result<()> {
        self.insert_queue
            .push(UserOperation::DeleteByAddress(segment_id, doc_id));
        if self.insert_queue.len() >= MAX_INSERT_QUEUE_SIZE {
            self.drain_insert_queue()?;
        }
        Ok(())
    }

    pub fn insert(&mut self, document: SearchDocument, ctid: u64) -> Result<()> {
        let mut tantivy_document: TantivyDocument = document.into();

        tantivy_document.add_u64(self.ctid_field, ctid);

        self.insert_queue.push(UserOperation::Add(tantivy_document));

        if self.insert_queue.len() >= MAX_INSERT_QUEUE_SIZE {
            self.drain_insert_queue()?;
        }
        Ok(())
    }

    pub fn insert_as_block(
        &mut self,
        child_documents: Vec<SearchDocument>,
        parent_document: SearchDocument,
        ctid: u64,
    ) -> Result<()> {
        let mut tantivy_docs: Vec<TantivyDocument> =
            child_documents.into_iter().map(|doc| doc.into()).collect();

        for doc in &mut tantivy_docs {
            doc.add_u64(self.ctid_field, ctid);
        }

        let mut parent_doc: TantivyDocument = parent_document.into();
        parent_doc.add_u64(self.ctid_field, ctid);

        // Parent document must be added last.
        tantivy_docs.push(parent_doc);

        // All docs must be added contiguously, so we add them all
        // and then immediately flush the insert queue.
        for doc in tantivy_docs {
            self.insert_queue.push(UserOperation::Add(doc));
        }

        self.drain_insert_queue()?;
        Ok(())
    }

    pub fn commit(mut self) -> Result<()> {
        self.drain_insert_queue()?;
        let mut writer =
            Arc::into_inner(self.writer).expect("should not have an outstanding Arc<IndexWriter>");

        self.handler
            .wait_for(move || {
                let opstamp = writer.commit()?;
                writer.wait_merging_threads()?;
                tantivy::Result::Ok(opstamp)
            })
            .expect("spawned thread should not fail")?;

        Ok(())
    }

    fn drain_insert_queue(&mut self) -> Result<Opstamp, TantivyError> {
        let insert_queue = std::mem::take(&mut self.insert_queue);
        let writer = self.writer.clone();
        self.handler
            .wait_for(move || writer.run(insert_queue))
            .expect("spawned thread should not fail")
    }
}

#[derive(Error, Debug)]
pub enum IndexError {
    #[error(transparent)]
    TantivyError(#[from] tantivy::TantivyError),

    #[error(transparent)]
    IOError(#[from] std::io::Error),

    #[error(transparent)]
    SerdeJsonError(#[from] serde_json::Error),

    #[error(transparent)]
    TantivyValueError(#[from] TantivyValueError),

    #[error("key_field column '{0}' cannot be NULL")]
    KeyIdNull(String),
}
