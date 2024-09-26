// Copyright (c) 2023-2024 Retake, Inc.
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

use crate::{
    index::SearchIndex,
    postgres::types::TantivyValueError,
    schema::{
        SearchDocument, SearchFieldConfig, SearchFieldName, SearchFieldType, SearchIndexSchema,
    },
};
use anyhow::{Context, Result};
use std::sync::Arc;
use std::{
    collections::{
        hash_map::Entry::{Occupied, Vacant},
        HashMap, HashSet,
    },
    path::Path,
};
use std::{fs, io, result};
use tantivy::directory::{
    DirectoryClone, DirectoryLock, FileHandle, FileSlice, Lock, WatchCallback, WatchHandle,
    WritePtr,
};
use tantivy::{
    directory::error::{DeleteError, LockError, OpenReadError, OpenWriteError},
    IndexSettings,
};
use tantivy::{directory::MmapDirectory, schema::Field, Directory, Index, IndexWriter};
use thiserror::Error;
use tracing::warn;

use super::directory::{SearchDirectoryError, SearchFs, WriterDirectory};

/// We maintain our own tantivy::directory::Directory implementation for finer-grained
/// control over the locking behavior, which enables us to manage Writer instances
/// across multiple connections.
#[derive(Debug)]
pub struct BlockingDirectory(MmapDirectory);

impl BlockingDirectory {
    pub fn open(directory_path: impl AsRef<Path>) -> Result<Self> {
        if !directory_path.as_ref().exists() {
            fs::create_dir_all(&directory_path).expect("must be able to create index directory")
        }
        Ok(Self(MmapDirectory::open(directory_path)?))
    }
}

impl DirectoryClone for BlockingDirectory {
    fn box_clone(&self) -> Box<dyn Directory> {
        self.0.box_clone()
    }
}

impl Directory for BlockingDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        self.0.get_file_handle(path)
    }

    fn open_read(&self, path: &Path) -> result::Result<FileSlice, OpenReadError> {
        self.0.open_read(path)
    }

    fn open_write(&self, path: &Path) -> result::Result<WritePtr, OpenWriteError> {
        self.0.open_write(path)
    }

    fn atomic_write(&self, path: &Path, data: &[u8]) -> io::Result<()> {
        self.0.atomic_write(path, data)
    }

    fn atomic_read(&self, path: &Path) -> result::Result<Vec<u8>, OpenReadError> {
        self.0.atomic_read(path)
    }

    fn delete(&self, path: &Path) -> result::Result<(), DeleteError> {
        self.0.delete(path)
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        self.0.exists(path)
    }

    fn acquire_lock(&self, lock: &Lock) -> result::Result<DirectoryLock, LockError> {
        // This is the only change we actually need to make to the Directory trait impl.
        // We want the acquire_lock behavior to block and wait for a lock to be available,
        // instead of panicking. Internally, Tantivy just polls for its availability.
        let blocking_lock = Lock {
            filepath: lock.filepath.clone(),
            is_blocking: true,
        };
        self.0.acquire_lock(&blocking_lock)
    }

    fn watch(&self, watch_callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        self.0.watch(watch_callback)
    }

    fn sync_directory(&self) -> io::Result<()> {
        self.0.sync_directory()
    }
}

/// The entity that interfaces with Tantivy indexes.
#[derive(Default)]
pub struct WriterManager {
    /// Map of index directory path to Tantivy writer instance.
    tantivy_writers: HashMap<WriterDirectory, IndexWriter>,
    drop_requested: HashSet<WriterDirectory>,
}

impl WriterManager {
    pub fn new() -> Self {
        Self {
            tantivy_writers: HashMap::new(),
            drop_requested: HashSet::new(),
        }
    }

    /// Check the writer server cache for an existing IndexWriter. If it does not exist,
    /// then retrieve the SearchIndex and use it to create a new IndexWriter, caching it.
    fn get_writer(&mut self, directory: WriterDirectory) -> Result<&mut IndexWriter, IndexError> {
        match self.tantivy_writers.entry(directory.clone()) {
            Vacant(entry) => {
                Ok(entry.insert(SearchIndex::writer(&directory).map_err(|err| {
                    IndexError::GetWriterFailed(directory.clone(), err.to_string())
                })?))
            }
            Occupied(entry) => Ok(entry.into_mut()),
        }
    }

    pub fn clear_writers(&mut self) {
        self.tantivy_writers.clear();
    }

    pub fn insert(
        &mut self,
        directory: WriterDirectory,
        document: SearchDocument,
    ) -> Result<(), IndexError> {
        let writer = self.get_writer(directory)?;
        // Add the Tantivy document to the index.
        writer.add_document(document.into())?;

        Ok(())
    }

    pub fn delete(
        &mut self,
        directory: WriterDirectory,
        ctid_field: &Field,
        ctid_values: &[u64],
    ) -> Result<(), IndexError> {
        let writer = self.get_writer(directory)?;
        for ctid in ctid_values {
            let ctid_term = tantivy::Term::from_field_u64(*ctid_field, *ctid);
            writer.delete_term(ctid_term);
        }
        Ok(())
    }

    pub fn commit(&mut self, directory: WriterDirectory) -> Result<()> {
        if directory.exists()? {
            let writer = self.get_writer(directory.clone())?;

            writer
                .prepare_commit()
                .context("error preparing commit to tantivy index")?;
            writer
                .commit()
                .context("error committing to tantivy index")?;
        } else {
            warn!(?directory, "index directory unexpectedly does not exist");
        }

        if self.drop_requested.contains(&directory) {
            // The directory has been dropped in this transaction. Now that we're
            // committing, we must physically delete it.
            self.drop_requested.remove(&directory);
            self.drop_index_on_commit(directory)?
        }
        Ok(())
    }

    pub fn abort(&mut self, directory: WriterDirectory) -> Result<(), IndexError> {
        // If the transaction was aborted, we should roll back the writer to the last commit.
        // Otherwise, partially written data could stick around for the next transaction.
        if let Some(writer) = self.tantivy_writers.get_mut(&directory) {
            writer.rollback()?;
        }

        // If the index was dropped in this transaction, we will not actually physically
        // delete the files, as we've aborted. Remove the directory from the
        // drop_requested set.
        self.drop_requested.remove(&directory);

        Ok(())
    }

    pub fn vacuum(&mut self, directory: WriterDirectory) -> Result<(), IndexError> {
        let writer = self.get_writer(directory)?;
        writer.garbage_collect_files().wait()?;
        Ok(())
    }

    pub fn create_index(
        &mut self,
        directory: WriterDirectory,
        fields: Vec<(SearchFieldName, SearchFieldConfig, SearchFieldType)>,
        uuid: String,
        key_field_index: usize,
    ) -> Result<()> {
        let schema = SearchIndexSchema::new(fields, key_field_index)?;

        let tantivy_dir_path = directory.tantivy_dir_path(true)?;
        let tantivy_dir = BlockingDirectory::open(tantivy_dir_path)?;
        let mut underlying_index =
            Index::create(tantivy_dir, schema.schema.clone(), IndexSettings::default())?;

        SearchIndex::setup_tokenizers(&mut underlying_index, &schema);

        let new_self = SearchIndex {
            reader: SearchIndex::reader(&underlying_index)?,
            underlying_index,
            directory: directory.clone(),
            schema,
            uuid,
            is_dirty: false,
            is_pending_drop: false,
            is_pending_create: true,
        };

        // Serialize SearchIndex to disk so it can be initialized by other connections.
        new_self.directory.save_index(&new_self)?;
        Ok(())
    }

    /// Physically delete the Tantivy directory. This should only be called on commit.
    fn drop_index_on_commit(&mut self, directory: WriterDirectory) -> Result<()> {
        if let Some(writer) = self.tantivy_writers.remove(&directory) {
            std::mem::drop(writer);
        };

        directory.remove()?;
        Ok(())
    }

    /// Handle a request to drop an index. We don't physically delete the files in this
    /// function in case the transaction is aborted. Instead, we mark the directory for
    /// deletion upon commit.
    pub fn drop_index(&mut self, directory: WriterDirectory) -> Result<(), IndexError> {
        self.drop_requested.insert(directory);
        Ok(())
    }
}

#[derive(Error, Debug)]
pub enum IndexError {
    #[error("couldn't get writer for {0:?}: {1}")]
    GetWriterFailed(WriterDirectory, String),

    #[error(transparent)]
    TantivyError(#[from] tantivy::TantivyError),

    #[error(transparent)]
    IOError(#[from] std::io::Error),

    #[error(transparent)]
    SerdeJsonError(#[from] serde_json::Error),

    #[error(transparent)]
    TantivyValueError(#[from] TantivyValueError),

    #[error("couldn't remove index files on drop_index: {0}")]
    DeleteDirectory(#[from] SearchDirectoryError),

    #[error("key_field column '{0}' cannot be NULL")]
    KeyIdNull(String),
}
