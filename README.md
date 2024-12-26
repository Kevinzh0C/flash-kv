<div align="center">
<h1>Flash-KV</h1>
</div>

<div align="center">

[<img alt="github" src="https://img.shields.io/badge/github-Kevinzh0C%2Fflash--kv-8da0cb?style=for-the-badge&logo=GitHub&label=github&color=8da0cb" height="22">][Github-url]
[<img alt="Build" src="https://img.shields.io/github/actions/workflow/status/Kevinzh0C/FlashKV-rs/rust.yml?branch=main&style=for-the-badge&logo=Github-Actions" height="22">][CI-url]
[<img alt="Codecov" src="https://img.shields.io/codecov/c/gh/Kevinzh0C/FlashKV-rs?style=for-the-badge&logo=codecov" height="22">][codecov-url]
[<img alt="GitHub License" src="https://img.shields.io/github/license/Kevinzh0C/FlashKV-rs?style=for-the-badge&logo=license&label=license" height="22">][License-url]

An efficient key-value storage engine, designed for fast reading and writing, which is inspired by [Bitcask][bitcask_url].

See [Introduction](#introduction), [Installation](#installation) and [Usages](#usages) for more details.

</div>

## Introduction

Flash-KV is a high-performance key-value storage system written in Rust. It leverages a log-structured design with an append-only write approach to deliver exceptional speed, reliability, and scalability.

### Features

- **Efficient Key-Value Storage:** Optimized for fast read and write operations with minimal overhead.
- **Diverse Index:** Support BTree, Skiplist, BPlusTree index for multiple index strategies.
- **MemMap files for efficient I/O:**  To achieve rapid index reconstruction and enhance startup speeds
- **Low latency per item read or written:** Benchmarks run on a Macintosh with Apple M1 Core:
    - Write latency:  `~ 3.3 µs`
    - Read latency:  `~ 370 ns` 
- **Concurrency Support:**   fine-grained locking minimizes contentions.
- **WriteBatch transaction:**   commit a batch of writes to ensure atomicity.


## Installation

To use flash-kv in your project, add it as a dependency in your Cargo.toml file:

  ```toml
  [dependencies]
  flash-kv = "0.2.1"
  ```
Then, run cargo build to download and compile flash-kv and its dependencies.

For more detailed setup and compilation instructions, visit the Flash-KV GitHub repository.

## Usages
Please see [`examples`].

For detailed usage and API documentation, refer to the [flash-kv Documentation](https://docs.rs/flash-kv).

## TODO

- [X] Basic error handling
- [X] Merge files during compaction
- [X] Configurable compaction triggers and thresholds
- [X] WriteBactch transaction
- [X] Use mmap to read data file that on disk.
- [X] Optimize hintfile storage structure to support the memtable build faster 
- [X] Http api server
- [X] Tests
- [X] Benchmark
- [ ] Documentation 
- [ ] Increased use of flatbuffers option to support faster reading speed
- [ ] Extend support for Redis Data Types

## Contribution

Contributions to this project are welcome! If you find any issues or have suggestions for improvements, please raise an issue or submit a pull request.


#### License

<sup>
Flash-KV is licensed under the [MIT license](https://github.com/KevinZh0C/FlashKV-rs/blob/main/LICENSE-MIT), permitting use in both open source and private projects.
</sup>
<br>
<sub>
This license grants you the freedom to use flash-kv in your own projects, under the condition that the original license and copyright notice are included with any substantial portions of the Flash-KV software.
</sub>


[Github-url]: https://github.com/KevinZh0C/FlashKV-rs
[CI-url]: https://github.com/Kevinzh0C/FlashKV-rs/actions/workflows/rust.yml
[doc-url]: https://docs.rs/flash-kv

[crates-url]: https://crates.io/crates/flash-kv
[codecov-url]: https://app.codecov.io/gh/KevinZh0C/FlashKV-rs
[bitcask_url]: https://riak.com/assets/bitcask-intro.pdf
[`examples`]: https://github.com/KevinZh0C/FlashKV-rs/tree/main/examples
[License-url]: LICENSE
