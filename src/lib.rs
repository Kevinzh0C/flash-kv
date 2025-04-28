//! Flash-KV: A high-performance key-value storage engine inspired by Bitcask.
//! 
//! Flash-KV implements a log-structured storage design optimized for fast reads and writes.
//! The engine provides efficient storage and retrieval of key-value pairs with durability
//! guarantees and space management through compaction.
//!
//! # Features
//! 
//! * Fast reads and writes with minimal disk I/O
//! * Durable storage with configurable sync options
//! * Atomic write batches for transactional operations
//! * Efficient space reclamation through compaction
//! * Multiple index implementations for different performance needs
//! * Memory-mapped I/O support for improved performance
//!
//! # Basic Usage
//!
//! ```
//! use bytes::Bytes;
//! use flash_kv::{db::Engine, option::Options};
//!
//! // Create a default engine instance
//! let opts = Options::default();
//! let engine = Engine::open(opts).expect("Failed to open flash-kv engine");
//!
//! // Store a key-value pair
//! let key = Bytes::from(b"hello".to_vec());
//! let value = Bytes::from(b"world".to_vec());
//! engine.put(key.clone(), value.clone()).expect("Failed to put");
//!
//! // Retrieve the value
//! let retrieved = engine.get(key.clone()).expect("Failed to get");
//! assert_eq!(retrieved, value);
//!
//! // Delete the key
//! engine.delete(key).expect("Failed to delete");
//! ```

mod data;

mod fio;
mod index;
mod iterator;

pub mod batch;
pub mod db;
#[cfg(test)]
mod db_test;
pub mod errors;
pub mod merge;
pub mod option;
pub mod util;
