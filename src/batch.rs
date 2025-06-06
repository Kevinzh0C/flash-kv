use std::{
  collections::HashMap,
  sync::{atomic::Ordering, Arc},
};

use bytes::{BufMut, Bytes, BytesMut};
use parking_lot::Mutex;
use prost::{decode_length_delimiter, encode_length_delimiter};

use crate::{
  data::log_record::{LogRecord, LogRecordType},
  db::Engine,
  errors::{Errors, Result},
  option::{IndexType, WriteBatchOptions},
};

const TXN_FIN_KEY: &[u8] = "txn-fin".as_bytes();
pub(crate) const NON_TXN_SEQ_NO: usize = 0;

/// A batch of write operations ensuring atomicity and consistency.
pub struct WriteBatch<'a> {
  pending_writes: Arc<Mutex<HashMap<Vec<u8>, LogRecord>>>, // temporarily store the write data
  engine: &'a Engine,
  options: WriteBatchOptions,
}

impl Engine {
  /// Creates a new write batch for grouped operations.
  /// * `options` - Configuration options for the write batch.
  pub fn new_write_batch(&self, options: WriteBatchOptions) -> Result<WriteBatch> {
    if self.options.index_type == IndexType::BPlusTree && !self.seq_file_exists && !self.is_initial
    {
      return Err(Errors::UnableToUseWriteBatch);
    }

    Ok(WriteBatch {
      pending_writes: Arc::new(Mutex::new(HashMap::new())),
      engine: self,
      options,
    })
  }
}

impl WriteBatch<'_> {
  /// Adds a key-value pair to the write batch.
  pub fn put(&self, key: Bytes, value: Bytes) -> Result<()> {
    if key.is_empty() {
      return Err(Errors::KeyIsEmpty);
    }

    // pending write
    let record = LogRecord {
      key: key.to_vec(),
      value: value.to_vec(),
      rec_type: LogRecordType::Normal,
    };

    let mut pending_writes = self.pending_writes.lock();
    pending_writes.insert(key.to_vec(), record);
    Ok(())
  }

  /// Marks a key for deletion in the write batch.
  pub fn delete(&self, key: Bytes) -> Result<()> {
    if key.is_empty() {
      return Err(Errors::KeyIsEmpty);
    }

    let mut pending_writes = self.pending_writes.lock();
    // if data not exist, just return
    let index_pos = self.engine.index.get(key.to_vec());
    if index_pos.is_none() {
      if pending_writes.contains_key(&key.to_vec()) {
        pending_writes.remove(&key.to_vec());
      }
      return Ok(());
    }

    // pending delete
    let record = LogRecord {
      key: key.to_vec(),
      value: Default::default(),
      rec_type: LogRecordType::Deleted,
    };
    pending_writes.insert(key.to_vec(), record);
    Ok(())
  }

  pub fn commit(&self) -> Result<()> {
    let mut pending_writes = self.pending_writes.lock();
    if pending_writes.is_empty() {
      return Ok(());
    }
    if pending_writes.len() > self.options.max_batch_num {
      return Err(Errors::ExceedMaxBatchNum);
    }

    // mutex lock the engine to ensure serial write
    let _lock = self.engine.batch_commit_lock.lock();

    // obtain txn id
    let seq_no = self.engine.seq_no.fetch_add(1, Ordering::SeqCst);

    let mut positions = HashMap::new();
    // start write to data file
    for (_, item) in pending_writes.iter() {
      let mut record = LogRecord {
        key: log_record_key_with_seq(item.key.clone(), seq_no),
        value: item.value.clone(),
        rec_type: item.rec_type,
      };

      let pos = self.engine.append_log_record(&mut record)?;
      positions.insert(item.key.clone(), pos);
    }

    // last write txn finished record
    let mut finish_record = LogRecord {
      key: log_record_key_with_seq(TXN_FIN_KEY.to_vec(), seq_no),
      value: Default::default(),
      rec_type: LogRecordType::TxnFinished,
    };

    // if sync writes configs, sync data file
    self.engine.append_log_record(&mut finish_record)?;
    if self.options.sync_writes {
      self.engine.sync()?;
    }

    // after write, update index
    for (_, item) in pending_writes.iter() {
      let record_pos = positions.get(&item.key).unwrap();
      if item.rec_type == LogRecordType::Normal {
        if let Some(old_pos) = self.engine.index.put(item.key.clone(), *record_pos) {
          self
            .engine
            .reclaim_size
            .fetch_add(old_pos.size as usize, Ordering::SeqCst);
        }
      }
      if item.rec_type == LogRecordType::Deleted {
        if let Some(old_pos) = self.engine.index.delete(item.key.clone()) {
          self
            .engine
            .reclaim_size
            .fetch_add(old_pos.size as usize, Ordering::SeqCst);
        }
      }
    }

    // clear pending writes for next commit
    pending_writes.clear();

    Ok(())
  }
}

pub(crate) fn log_record_key_with_seq(key: Vec<u8>, seq_no: usize) -> Vec<u8> {
  let mut enc_key = BytesMut::new();
  encode_length_delimiter(seq_no, &mut enc_key).unwrap();
  enc_key.extend_from_slice(&key.to_vec());
  enc_key.to_vec()
}

pub(crate) fn parse_log_record_key(key: Vec<u8>) -> (Vec<u8>, usize) {
  let mut buf = BytesMut::new();
  buf.put_slice(&key);
  let seq_no = decode_length_delimiter(&mut buf).unwrap();
  (buf.to_vec(), seq_no)
}

#[cfg(test)]
mod tests {
  use tempfile::tempdir;

  use crate::{
    option::Options,
    util::rand_kv::{get_test_key, get_test_value},
  };

  use super::*;

  #[test]
  fn test_write_batch_1() {
    let temp_dir = tempdir().expect("failed to create temp dir");
    let mut opt = Options::default();
    opt.dir_path = temp_dir.path().to_path_buf();
    opt.data_file_size = 64 * 1024 * 1024; // 64MB
    let engine = Engine::open(opt.clone()).expect("fail to open engine");

    let wb = engine
      .new_write_batch(WriteBatchOptions::default())
      .expect("fail to create write batch");

    // uncommitted put
    let put_res1 = wb.put(get_test_key(1), get_test_value(10));
    assert!(put_res1.is_ok());
    let put_res2 = wb.put(get_test_key(2), get_test_value(20));
    assert!(put_res2.is_ok());

    let res1 = engine.get(get_test_key(1));
    assert_eq!(Errors::KeyNotFound, res1.err().unwrap());

    // query after transaction commit
    let commit_res = wb.commit();
    assert!(commit_res.is_ok());
    let res2 = engine.get(get_test_key(1));
    assert_eq!(get_test_value(10), res2.unwrap());

    // verify sequence number
    let seq_no = wb.engine.seq_no.load(Ordering::SeqCst);
    assert_eq!(2, seq_no);
  }

  #[test]
  fn test_write_batch_2() {
    let temp_dir = tempdir().expect("failed to create temp dir");
    let mut opt = Options::default();
    opt.dir_path = temp_dir.path().to_path_buf();
    opt.data_file_size = 64 * 1024 * 1024; // 64MB
    let engine = Engine::open(opt.clone()).expect("fail to open engine");

    let wb = engine
      .new_write_batch(WriteBatchOptions::default())
      .expect("fail to create write batch");

    let put_res1 = wb.put(get_test_key(1), get_test_value(10));
    assert!(put_res1.is_ok());
    let put_res2 = wb.put(get_test_key(2), get_test_value(20));
    assert!(put_res2.is_ok());
    let commit_res1 = wb.commit();
    assert!(commit_res1.is_ok());

    let put_res3 = wb.put(get_test_key(3), get_test_value(10));
    assert!(put_res3.is_ok());
    let commit_res2 = wb.commit();
    assert!(commit_res2.is_ok());

    // verify sequence number after restart
    engine.close().expect("fail to close");
    let engine2 = Engine::open(opt.clone()).expect("fail to open engine");
    let keys = engine2.list_keys();
    assert_eq!(3, keys.unwrap().len());
    let seq_no = engine2.seq_no.load(Ordering::SeqCst);
    assert_eq!(3, seq_no);
  }

  #[test]
  fn test_write_batch_3() {
    let temp_dir = tempdir().expect("failed to create temp dir");
    let mut opt = Options::default();
    opt.dir_path = temp_dir.path().to_path_buf();
    opt.data_file_size = 64 * 1024 * 1024; // 64MB
    let engine = Engine::open(opt.clone()).expect("fail to open engine");

    let mut wb_opts = WriteBatchOptions::default();
    wb_opts.max_batch_num = 10000000;
    let wb = engine
      .new_write_batch(wb_opts)
      .expect("fail to create write batch");

    for i in 0..=1000000 {
      let put_res = wb.put(get_test_key(i), get_test_value(i));
      assert!(put_res.is_ok());
    }

    let commit_res1 = wb.commit();
    assert!(commit_res1.is_ok());
  }
}
