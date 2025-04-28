pub mod file_io;
pub mod mmap;

use std::path::PathBuf;

use crate::{errors::Result, option::IOManagerType};

use self::{file_io::FileIO, mmap::MMapIO};

/// Abstract I/O management interface for different I/O implementations.
pub trait IOManager: Sync + Send {
  fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize>;

  fn write(&self, buf: &[u8]) -> Result<usize>;

  fn sync(&self) -> Result<()>;

  fn size(&self) -> u64;
}

pub fn new_io_manager(filename: &PathBuf, io_type: &IOManagerType) -> Box<dyn IOManager> {
  match *io_type {
    IOManagerType::StandardFileIO => Box::new(FileIO::new(filename).unwrap()),
    IOManagerType::MemoryMap => Box::new(MMapIO::new(filename).unwrap()),
  }
}
