use std::{fs::OpenOptions, path::Path, sync::Arc};

use log::error;
use memmap2::Mmap;
use parking_lot::Mutex;

use crate::errors::{Errors, Result};

use super::IOManager;

pub struct MMapIO {
  //
  map: Arc<Mutex<Mmap>>,
}

impl MMapIO {
  pub fn new<P>(file_name: P) -> Result<Self>
  where
    P: AsRef<Path>,
  {
    match OpenOptions::new()
      .create(true)
      .read(true)
      .append(true)
      .open(file_name)
    {
      Ok(file) => {
        let map = unsafe { Mmap::map(&file).expect("failed to map file") };
        Ok(MMapIO {
          map: Arc::new(Mutex::new(map)),
        })
      }
      Err(e) => {
        error!("failed to open data file error: {e}");
        Err(Errors::FailedToOpenDataFile)
      }
    }
  }
}

impl IOManager for MMapIO {
  fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
    let map_arr = self.map.lock();
    let end = offset + buf.len() as u64;
    if end > map_arr.len() as u64 {
      return Err(Errors::ReadDataFileEOF);
    }

    let val = &map_arr[offset as usize..end as usize];
    buf.copy_from_slice(val);

    Ok(val.len())
  }

  fn write(&self, _buf: &[u8]) -> Result<usize> {
    unimplemented!()
  }

  fn sync(&self) -> Result<()> {
    unimplemented!()
  }

  fn size(&self) -> u64 {
    let map_arr = self.map.lock();
    map_arr.len() as u64
  }
}

#[cfg(test)]
mod tests {
  use std::fs;
  use tempfile::tempdir;

  use crate::fio::file_io::FileIO;

  use super::*;

  #[test]
  fn test_mmap_read() {
    let temp_dir = tempdir().expect("failed to create temp dir for mmap_read test");
    let path = temp_dir.path().join("mmap-test.data");

    let _ = fs::remove_file(&path);

    let file = OpenOptions::new()
      .create(true)
      .write(true)
      .truncate(true)
      .open(&path)
      .unwrap();
    file.sync_all().unwrap();

    let mmap_res1 = MMapIO::new(&path);
    assert!(mmap_res1.is_ok());
    let mmap_io1 = mmap_res1.ok().unwrap();

    let mut buf1 = [0u8; 10];
    let read_res1 = mmap_io1.read(&mut buf1, 0);

    assert!(read_res1.is_err());

    let fio_res = FileIO::new(&path);
    assert!(fio_res.is_ok());
    let fio = fio_res.ok().unwrap();
    fio.write(b"hello world").unwrap();
    fio.write(b"good morning").unwrap();
    fio.write(b"seeyou again").unwrap();
    fio.sync().unwrap();

    let mmap_res2 = MMapIO::new(&path);
    assert!(mmap_res2.is_ok());
    let mmap_io2 = mmap_res2.ok().unwrap();

    let mut buf2 = [0u8; 35];
    let read_res2 = mmap_io2.read(&mut buf2, 0);
    assert!(read_res2.is_ok());
  }

  #[test]
  fn test_mmap_size() {
    let temp_dir = tempdir().expect("failed to create temp dir for mmap_size test");
    let path = temp_dir.path().join("mmap-test.data");

    let mmap_res1 = MMapIO::new(&path);
    assert!(mmap_res1.is_ok());
    let mmap_io1 = mmap_res1.ok().unwrap();
    let size1 = mmap_io1.size();
    assert_eq!(size1, 0);

    let fio_res = FileIO::new(&path);
    assert!(fio_res.is_ok());
    let fio = fio_res.ok().unwrap();
    fio.write(b"hello world").unwrap();
    fio.write(b"good morning").unwrap();
    fio.write(b"seeyou again").unwrap();
    fio.sync().unwrap();

    let mmap_res2 = MMapIO::new(&path);
    assert!(mmap_res2.is_ok());
    let mmap_io2 = mmap_res2.ok().unwrap();
    let size2 = mmap_io2.size();
    assert!(size2 > 0);
    assert_eq!(size2, 35);
  }
}
