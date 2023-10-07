use crate::partition::index::{FindResult, Index};
use crate::partition::log::Log;
use crate::partition::record::Record;
use crate::partition::LOG_MAX_SIZE;
use std::io::BufReader;
use std::path::Path;

#[derive(Debug)]
pub enum SegmentError {
    Io(std::io::Error),
    FullSegment,
}

#[derive(Debug)]
pub struct Segment {
    log: Log,
    index: Index,
    pub base_offset: u64,
    prev_offset: u64,
    offset_interval: usize,
    active: bool,
}

impl Segment {
    pub fn new(
        base_dir: &str,
        base_offset: u64,
        offset_interval: usize,
        active: bool,
    ) -> std::io::Result<Self> {
        let path = Path::new(base_dir).to_path_buf();
        let log = Log::new(&path, base_offset, LOG_MAX_SIZE)?;
        let index = Index::new(&path, base_offset, offset_interval)?;
        Ok(Self {
            log,
            index,
            base_offset,
            prev_offset: base_offset,
            offset_interval,
            active,
        })
    }

    pub fn load_from_disk(
        base_dir: &str,
        base_offset: u64,
        offset_interval: usize,
        active: bool,
    ) -> std::io::Result<Self> {
        let path = Path::new(base_dir).to_path_buf();
        let log = Log::load_from_disk(&path, base_offset, LOG_MAX_SIZE)?;
        let prev_offset = match log.current_offset {
            0 => 0,
            n if n < offset_interval as u64 => n,
            n if n % offset_interval as u64 == 0 => n - offset_interval as u64,
            n => n - (n % offset_interval as u64),
        };
        Ok(Self {
            log,
            index: Index::load_from_disk(&path, base_offset, offset_interval)?,
            base_offset,
            prev_offset,
            offset_interval,
            active,
        })
    }

    pub fn latest_offset(&self) -> u64 {
        self.log.current_offset
    }

    pub fn size(&self) -> usize {
        self.log.size
    }

    pub fn seal(&mut self) {
        self.active = false;
    }

    pub fn append_record(
        &mut self,
        key: Option<Vec<u8>>,
        value: &[u8],
    ) -> Result<(), SegmentError> {
        let record = Record::new(self.latest_offset(), key, value.to_vec());
        if !self.log.can_fit(record.binary_size()) {
            println!(
                "resizing: {} {} {}",
                self.base_offset,
                self.latest_offset(),
                self.size()
            );
            Err(SegmentError::FullSegment)
        } else {
            let mut buffer = Vec::with_capacity(record.binary_size());
            // let mut writer = BufWriter::new(vec![0u8; record.binary_size()]);
            record
                .write(&mut buffer)
                .map_err(|err| SegmentError::Io(err))?;
            match self.log.append_record(&buffer) {
                Ok((last_offset, log_size)) => {
                    if last_offset - self.prev_offset >= self.offset_interval as u64 {
                        self.index
                            .append_position(last_offset as u32, log_size)
                            .map_err(|err| SegmentError::Io(err))?;
                        self.prev_offset = last_offset;
                    }
                    self.log.flush().map_err(|err| SegmentError::Io(err))?;
                    self.index.flush().map_err(|err| SegmentError::Io(err))?;

                    Ok(())
                }
                Err(e) => Err(SegmentError::Io(e)),
            }
        }
    }

    pub fn read_at(&mut self, offset: u64) -> std::io::Result<Record> {
        match self.index.find_offset(offset as u32) {
            Ok(FindResult::Punctual(offset_position)) => {
                println!("{}", self.size());
                let slice = self
                    .log
                    .read_at(offset_position.position as usize, self.size())?;
                let mut reader = BufReader::new(slice);
                let mut record = Record::from_binary(&mut reader)?;
                if record.offset == offset {
                    Ok(record)
                } else {
                    let mut remaining_bytes =
                        self.size() - offset_position.position as usize - record.binary_size();
                    let mut stop = false;
                    while remaining_bytes > 0 && stop == false {
                        record = Record::from_binary(&mut reader)?;
                        remaining_bytes -= record.binary_size();
                        stop = record.offset == offset;
                    }
                    Ok(record)
                }
            }
            Ok(FindResult::Ahead((offset_position, next_offset))) => {
                let mut offset_count =
                    (offset - self.base_offset - offset_position.relative_offset as u64) + 1;
                let slice = self.log.read_at(
                    offset_position.position as usize,
                    next_offset.position as usize,
                )?;
                // log_file.seek(SeekFrom::Start(index_position.position as u64))?;
                let mut records: Vec<Record> = Vec::new();
                // let mut reader = BufReader::new(&log_file);
                let mut reader = BufReader::new(slice);
                while offset_count != 0 {
                    let r = Record::from_binary(&mut reader)?;
                    records.push(r);
                    offset_count -= 1;
                }
                Ok(records.last().unwrap().clone())
            }
            Err(e) => Err(e),
        }
    }
}
