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
            current_offset: record_count,
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

    pub fn get_reader(&self) -> Result<BufReader<&File>> {
        Ok(BufReader::new(&self.file))
    }
}
