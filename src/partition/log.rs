use crate::partition::record::Record;
use memmap2::MmapMut;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Result, Write};
use std::path::PathBuf;

#[derive(Debug)]
pub struct Log {
    file: File,
    mmap: MmapMut,
    max_size: usize,
    pub size: usize,
    pub base_offset: u64,
    pub current_offset: u64,
}

impl Log {
    pub fn new(path: &PathBuf, base_offset: u64, max_size: usize) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path.join(format!("{:020}.log", base_offset)))?;

        file.set_len(max_size as u64)?;
        let mmap = unsafe { MmapMut::map_mut(&file)? };

        Ok(Self {
            file,
            mmap,
            size: 0,
            max_size,
            base_offset,
            current_offset: base_offset,
        })
    }

    pub fn load_from_disk(path: &PathBuf, base_offset: u64, max_size: usize) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .create(false)
            .append(true)
            .open(path.join(format!("{:020}.log", base_offset)))?;
        let log_size = file.metadata().unwrap().len();
        let mut record_count = 0;
        let mut reader = BufReader::new(&file);
        // We read all the records from the log file till EOF and count them.
        //
        // TODO read the index file last offset and read only the remaining bytes from
        // the log file.
        loop {
            match Record::from_binary(&mut reader) {
                Ok(_r) => record_count += 1,
                Err(_) => break,
            }
        }

        file.set_len(max_size as u64)?;
        let mmap = unsafe { MmapMut::map_mut(&file)? };

        Ok(Self {
            file,
            mmap,
            size: log_size as usize,
            max_size,
            base_offset,
            current_offset: base_offset + record_count,
        })
    }

    pub fn flush(&mut self) -> Result<()> {
        self.mmap.flush_async()
    }

    pub fn can_fit(&self, buffer_size: usize) -> bool {
        (self.max_size - self.size) >= buffer_size
    }

    pub fn append_record(&mut self, record_data: &[u8]) -> Result<(u64, u32)> {
        let data_size = record_data.len();
        let written_bytes =
            (&mut self.mmap[(self.size)..(self.size + data_size)]).write(record_data)?;
        let size = self.size;

        self.size += written_bytes;
        let latest_offset = self.current_offset;
        self.current_offset += 1;
        Ok((latest_offset, size as u32))
    }

    pub fn read_at(&self, offset: usize, size: usize) -> Result<&[u8]> {
        Ok(&self.mmap[offset..size])
    }
}

#[cfg(test)]
mod log_tests {

    use super::Log;
    use std::fs;
    use std::path::Path;
    use tempdir::TempDir;

    #[test]
    fn test_new() {
        let tmp_dir = TempDir::new("test_tempdir").unwrap();
        let expected_file = tmp_dir.path().join("00000000000000000000.log");

        let log = Log::new(&tmp_dir.path().to_path_buf(), 0, 10).unwrap();

        assert!(expected_file.as_path().exists());
        assert_eq!(log.base_offset, 0);
        assert_eq!(log.current_offset, 0);
        assert_eq!(log.size, 0);
        tmp_dir.close().unwrap();
    }

    #[test]
    fn test_load_from_disk() {
        let tmp_dir = TempDir::new("test_tempdir").unwrap();
        let expected_file = tmp_dir.path().join("00000000000000000048.log");
        fs::File::create(&expected_file).unwrap();

        let log = Log::load_from_disk(&tmp_dir.path().to_path_buf(), 48, 10).unwrap();

        assert!(expected_file.as_path().exists());
        assert_eq!(log.base_offset, 48);
        assert_eq!(log.current_offset, 48);
        assert_eq!(log.size, 0);
        tmp_dir.close().unwrap();
    }

    #[test]
    #[should_panic]
    fn test_invalid_load_from_disk() {
        Log::new(&Path::new("dont-exist-dir").to_path_buf(), 0, 10).unwrap();
    }

    #[test]
    fn test_can_fit() {
        let tmp_dir = TempDir::new("test_tempdir").unwrap();
        let expected_file = tmp_dir.path().join("00000000000000000000.log");
        fs::File::create(&expected_file).unwrap();

        let log = Log::load_from_disk(&tmp_dir.path().to_path_buf(), 0, 10).unwrap();

        assert!(log.can_fit(10));
        assert!(log.can_fit(11) == false);
        tmp_dir.close().unwrap();
    }

    #[test]
    fn test_append_record() {
        let tmp_dir = TempDir::new("test_tempdir").unwrap();
        let expected_file = tmp_dir.path().join("00000000000000000000.log");
        fs::File::create(&expected_file).unwrap();

        let mut log = Log::new(&tmp_dir.path().to_path_buf(), 0, 34).unwrap();

        log.append_record(b"test-record-data").unwrap();

        assert_eq!(log.current_offset, 1);

        assert_eq!(
            fs::read_to_string(expected_file)
                .unwrap()
                .replace("\u{0}", ""),
            String::from("test-record-data")
        );

        log.append_record(b"test-record-data-2").unwrap();
        assert_eq!(log.current_offset, 2);
        assert_eq!(log.size, 34);
        tmp_dir.close().unwrap();
    }

    #[test]
    fn test_read_at() {
        let tmp_dir = TempDir::new("test_tempdir").unwrap();
        let expected_file = tmp_dir.path().join("00000000000000000000.log");
        fs::File::create(&expected_file).unwrap();

        let mut log = Log::new(&tmp_dir.path().to_path_buf(), 0, 20).unwrap();

        log.append_record(b"test-record-data").unwrap();

        assert_eq!(log.read_at(0, 16).unwrap(), b"test-record-data");
        assert_eq!(log.read_at(3, 8).unwrap(), b"t-rec");
        tmp_dir.close().unwrap();
    }
}
