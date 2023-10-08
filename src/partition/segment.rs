use crate::partition::index::Index;
use crate::partition::log::Log;
use crate::partition::record::Record;
use crate::partition::LOG_MAX_SIZE;
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
        let index = Index::new(&path, base_offset, offset_interval, LOG_MAX_SIZE / 2)?;
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
        let latest_offset = log.current_offset;
        let prev_offset = match latest_offset {
            0 => 0,
            n if n < offset_interval as u64 => n,
            n if n % offset_interval as u64 == 0 => n - offset_interval as u64,
            n => n - (n % offset_interval as u64),
        };
        Ok(Self {
            log,
            index: Index::load_from_disk(
                &path,
                base_offset,
                latest_offset,
                offset_interval,
                LOG_MAX_SIZE / 2,
            )?,
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

    pub fn flush(&mut self) -> std::io::Result<()> {
        self.log.flush()?;
        self.index.flush()
    }

    pub fn append_record(
        &mut self,
        key: Option<Vec<u8>>,
        value: &[u8],
    ) -> Result<(), SegmentError> {
        let record = Record::new(self.latest_offset(), key, value.to_vec());
        if !self.log.can_fit(record.binary_size()) {
            Err(SegmentError::FullSegment)
        } else {
            let mut buffer = Vec::with_capacity(record.binary_size());
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
                    Ok(())
                }
                Err(e) => Err(SegmentError::Io(e)),
            }
        }
    }

    pub fn read_at(&mut self, offset: u64) -> std::io::Result<Record> {
        match self.index.find_offset(offset as u32) {
            Ok(offset_range) => {
                let begin_relative_offset = offset_range.begin.relative_offset;
                let begin_position = offset_range.begin.position;
                let begin = if begin_relative_offset as u64 > (offset - self.base_offset) {
                    0
                } else {
                    begin_position as usize
                };
                let end = if offset_range.begin == offset_range.end {
                    self.size()
                } else {
                    offset_range.end.position as usize
                };
                let mut slice = self.log.read_at(begin, end)?;

                let mut offset_count = match offset {
                    0 => 1,
                    lesser if lesser < self.base_offset + begin_relative_offset as u64 => {
                        lesser - self.base_offset + 1
                    }
                    equal if equal == self.base_offset + begin_relative_offset as u64 => 1,
                    greater => (greater - self.base_offset - begin_relative_offset as u64) + 1,
                };

                let mut records: Vec<Record> = Vec::new();
                while offset_count != 0 {
                    let r = Record::from_binary(&mut slice)?;
                    records.push(r);
                    offset_count -= 1;
                }
                Ok(records.last().unwrap().clone())
            }
            Err(e) => Err(e),
        }
    }
}
