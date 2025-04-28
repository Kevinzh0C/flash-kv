use lazy_static::lazy_static;
use std::path::PathBuf;

lazy_static! {
  pub static ref DEFAULT_DIR_PATH: PathBuf = std::env::temp_dir().join("flash-kv");
}

#[derive(Debug, Clone)]
pub struct Options {
  pub dir_path: PathBuf,

  pub data_file_size: u64,

  pub sync_writes: bool,

  pub bytes_per_sync: usize,

  pub index_type: IndexType,

  pub mmap_at_startup: bool,

  pub file_merge_threshold: f32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexType {
  BTree,

  SkipList,

  BPlusTree,
}

impl Default for Options {
  fn default() -> Self {
    Self {
      dir_path: DEFAULT_DIR_PATH.clone(),
      data_file_size: 256 * 1024 * 1024, // 256MB
      sync_writes: false,
      bytes_per_sync: 0,
      index_type: IndexType::BTree,
      mmap_at_startup: true,
      file_merge_threshold: 0.6,
    }
  }
}
pub struct IteratorOptions {
  pub prefix: Vec<u8>,
  pub reverse: bool,
}

#[allow(clippy::derivable_impls)]
impl Default for IteratorOptions {
  fn default() -> Self {
    Self {
      prefix: Default::default(),
      reverse: false,
    }
  }
}

pub struct WriteBatchOptions {
  pub max_batch_num: usize,
  
  pub sync_writes: bool,
}

impl Default for WriteBatchOptions {
  fn default() -> Self {
    Self {
      max_batch_num: 1000,
      sync_writes: true,
    }
  }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IOManagerType {
  StandardFileIO,

  MemoryMap,
}
