use crate::log::record::Record;
use byteorder::{NetworkEndian, ReadBytesExt, WriteBytesExt};
use std::cmp::Ordering;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Result, Seek, SeekFrom, Write};
use std::path::Path;

const OFFSET_THRESHOLD: u64 = 10;

#[derive(Clone)]
pub struct Segment {
    log_file_path: String,
    idx_file_path: String,
    pub starting_offset: u64,
    prev_offset: u64,
    pub last_offset: u64,
    active: bool,
    size: usize,
}

enum SearchResult {
    Single(IndexPosition),
    Range((IndexPosition, IndexPosition)),
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct IndexPosition {
    offset: u32,
    position: u32,
}

impl IndexPosition {
    pub fn new(offset: u32, position: u32) -> Self {
        Self { offset, position }
    }

    pub fn write(&self, buf: &mut impl Write) -> Result<()> {
        buf.write_u32::<NetworkEndian>(self.offset)?;
        buf.write_u32::<NetworkEndian>(self.position)
    }

    pub fn from_binary(buf: &mut impl Read) -> Result<Self> {
        let offset = buf.read_u32::<NetworkEndian>()?;
        let position = buf.read_u32::<NetworkEndian>()?;
        Ok(Self { offset, position })
    }
}

impl Segment {
    pub fn new(basedir: &str, active: bool, starting_offset: u64) -> Result<Self> {
        let (log_path_str, idx_path_str) = Segment::path_strs(basedir, starting_offset);

        Ok(Self {
            log_file_path: log_path_str,
            idx_file_path: idx_path_str,
            starting_offset,
            prev_offset: starting_offset,
            last_offset: starting_offset,
            active,
            size: 0,
        })
    }

    pub fn from_disk(basedir: &str, active: bool, starting_offset: u64) -> Result<Self> {
        let (log_path_str, idx_path_str) = Segment::path_strs(basedir, starting_offset);
        let log_file = File::options().read(true).open(&log_path_str)?;
        let mut reader = BufReader::new(&log_file);
        let mut counter = 0;
        loop {
            match Record::from_binary(&mut reader) {
                Ok(_r) => counter += 1,
                Err(_) => break,
            }
        }
        let log_size = log_file.metadata().unwrap().len();
        let (prev_offset, last_offset) = match counter {
            0 => (0, 0),
            n if n < OFFSET_THRESHOLD => (n, n + 1),
            n if n % OFFSET_THRESHOLD == 0 => (n - OFFSET_THRESHOLD, n + 1),
            n => (n - (n % OFFSET_THRESHOLD), n + 1),
        };
        Ok(Self {
            log_file_path: log_path_str,
            idx_file_path: idx_path_str,
            starting_offset,
            prev_offset,
            last_offset,
            active,
            size: log_size as usize,
        })
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn seal(&mut self) {
        self.active = false;
    }

    pub fn append_record(&mut self, value: &[u8]) -> Result<()> {
        let record = Record::new(self.last_offset, value.to_vec());
        let log_file = File::options().append(true).open(&self.log_file_path)?;
        let mut writer = BufWriter::new(log_file);
        let record_bytes = record.write(&mut writer)?;
        self.last_offset += 1;
        if self.last_offset - self.prev_offset >= OFFSET_THRESHOLD {
            let offset = self.size;
            let index_file = File::options().append(true).open(&self.idx_file_path)?;
            let mut idx_writer = BufWriter::new(index_file);
            let index_position = IndexPosition::new(self.last_offset as u32, offset as u32);
            index_position.write(&mut idx_writer)?;
            self.prev_offset = self.last_offset;
        }
        self.size += record_bytes;
        Ok(())
    }

    pub fn read_at(&mut self, offset: u64) -> Result<Record> {
        match self.read_index(offset as u32) {
            Ok(SearchResult::Single(index_position)) => {
                let mut log_file = File::options().read(true).open(&self.log_file_path)?;
                log_file.seek(SeekFrom::Start(index_position.position as u64))?;
                let mut reader = BufReader::new(log_file);
                let r = Record::from_binary(&mut reader)?;
                Ok(r)
            }
            Ok(SearchResult::Range((index_position, next_position))) => {
                let offset_count = next_position.offset - index_position.offset;
                let mut log_file = File::options().read(true).open(&self.log_file_path)?;
                log_file.seek(SeekFrom::Start(index_position.position as u64))?;
                let mut records: Vec<Record> = Vec::new();
                let mut pointer = 0;
                let mut reader = BufReader::new(&log_file);
                while pointer < offset_count {
                    let r = Record::from_binary(&mut reader)?;
                    records.push(r);
                    pointer += 1;
                }
                let result_record = records.binary_search_by(|probe| probe.offset.cmp(&offset));
                match result_record {
                    Ok(i) => Ok(records[i].clone()),
                    Err(n) => panic!("Offset not found: {}", n),
                }
            }
            Err(e) => Err(e),
        }
    }

    fn read_index(&self, offset: u32) -> Result<SearchResult> {
        let mut idx_file = File::options().read(true).open(&self.idx_file_path)?;
        let mut buffer = Vec::new();
        idx_file.read_to_end(&mut buffer)?;
        let positions: Vec<IndexPosition> = buffer
            .chunks(8)
            .map(|mut c| IndexPosition::from_binary(&mut c).unwrap())
            .collect();

        let position =
            positions.binary_search_by(|probe| probe.offset.cmp(&offset).then(Ordering::Less));
        match position {
            Ok(pos) => {
                let index_position = &positions[pos];
                Ok(SearchResult::Single(*index_position))
            }
            Err(0) => {
                let next_position = &positions[0];
                Ok(SearchResult::Range((
                    IndexPosition::new(0, 0),
                    *next_position,
                )))
            }
            Err(off) => {
                let index_position = &positions[off - 1];
                if offset as u64 % OFFSET_THRESHOLD == 0 {
                    Ok(SearchResult::Single(*index_position))
                } else {
                    let next_position = &positions[off];
                    Ok(SearchResult::Range((*index_position, *next_position)))
                }
            }
        }
    }

    fn path_strs(basedir: &str, starting_offset: u64) -> (String, String) {
        let log_path = Path::new(basedir).join(format!("{:020}.log", &starting_offset));
        let idx_path = Path::new(basedir).join(format!("{:020}.index", &starting_offset));
        let log_path_str = log_path.into_os_string().into_string().unwrap();
        let idx_path_str = idx_path.to_str().unwrap().to_owned();
        (log_path_str, idx_path_str)
    }
}

#[cfg(test)]
mod index_position_tests {
    use super::IndexPosition;
    use std::io::BufReader;

    #[test]
    fn test_new() {
        let idx_position = IndexPosition::new(0, 0);
        assert_eq!(
            idx_position,
            IndexPosition {
                offset: 0,
                position: 0
            }
        );
    }

    #[test]
    fn test_write() {
        let idx_position = IndexPosition::new(0, 0);
        let mut buffer = vec![];
        idx_position.write(&mut buffer).unwrap();
        let mut reader = BufReader::new(&buffer[..]);
        let expected = IndexPosition::from_binary(&mut reader).unwrap();
        assert_eq!(idx_position, expected,);
    }
}
