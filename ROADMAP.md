# FlashKV Roadmap

This document outlines the planned features and improvements for the FlashKV project.

## Completed Features

- Core Features
  - Basic error handling
  - Merge files during compaction
  - Configurable compaction triggers and thresholds
  - WriteBatch transaction
  - Memory-mapped file I/O
  - Optimized hint file storage structure
  - HTTP API server
  - Comprehensive tests
  - Performance benchmarks

## In Progress

- Documentation improvements
- Increased use of flatbuffers option to support faster reading speed
- Extended support for Redis Data Types

## Planned Features

### OCC+2PL Transaction Model Implementation

#### Optimistic Concurrency Control (OCC) for Reads
- Add version control mechanism to each key-value pair 
- Implement a global clock in the `Engine` structure
- Create a new `ReadTransaction` structure to track read sets
- Modify the `get` method to support multi-version reads
- Implement a validation phase to check if read sets have been modified

#### Optimized Two-Phase Locking (2PL) for Writes
- Create a new `LockManager` module supporting intention locks, shared locks, and exclusive locks
- Implement key-level fine-grained locking
- Modify `WriteBatch` to implement the two-phase locking protocol
- Add lock acquisition in the growing phase
- Implement lock release after transaction completion
- Add deadlock detection mechanisms (timeout or dependency graph)

### Distributed System Support

- Implement a distributed consensus protocol (Raft or Paxos)
- Add data partitioning strategies
- Create a distributed transaction coordinator
- Support two-phase commit (2PC) protocol
- Develop an efficient inter-node communication layer
- Implement handling for network partitions and failures

## Technical Challenges to Address

- Storage format modifications to support version information
- Index extension to support multi-versioning and lock information
- Performance balancing between concurrency and transaction overhead
- Distributed consistency with acceptable performance
