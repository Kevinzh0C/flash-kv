pub mod bptree;
pub mod btree;
pub mod skiplist;

use std::path::PathBuf;

use bytes::Bytes;

use crate::{
  data::log_record::LogRecordPos,
  errors::Result,
  option::{IndexType, IteratorOptions},
};

pub trait Indexer: Sync + Send {
  fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> Option<LogRecordPos>;

  /// Retrieves a key's position from the index.
  fn get(&self, key: Vec<u8>) -> Option<LogRecordPos>;

  /// Deletes a key's position from the index.
  fn delete(&self, key: Vec<u8>) -> Option<LogRecordPos>;

  fn list_keys(&self) -> Result<Vec<Bytes>>;

  /// Creates an iterator for the index with the specified options.
  /// * `options` - Configuration options for the iterator
  fn iterator(&self, options: IteratorOptions) -> Box<dyn IndexIterator>;
}

/// Creates a new indexer based on the specified index type and directory path.
pub fn new_indexer(index_type: &IndexType, dir_path: &PathBuf) -> Box<dyn Indexer> {
  match *index_type {
    IndexType::BTree => Box::new(btree::BTree::new()),
    IndexType::SkipList => Box::new(skiplist::SkipList::new()),
    IndexType::BPlusTree => Box::new(bptree::BPlusTree::new(dir_path)),
  }
}

/// Provides methods for iterating over key-value pairs in the index.
pub trait IndexIterator: Sync + Send {
  fn rewind(&mut self);

  fn seek(&mut self, key: Vec<u8>);

  fn next(&mut self) -> Option<(&Vec<u8>, &LogRecordPos)>;
}
