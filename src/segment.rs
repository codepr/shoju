use crate::record;
use byteorder::{NetworkEndian, ReadBytesExt, WriteBytesExt};
use memmap::MmapOptions;
use std::cmp::Ordering;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Result, Seek, SeekFrom, Write};
use std::path::Path;

const LOG_PATH: &str = "logdir";
const OFFSET_THRESHOLD: u64 = 10;

pub struct Segment {
    log_file: File,
    index_file: File,
    prev_offset: u64,
    last_offset: u64,
    active: bool,
}

enum SearchResult {
    Single(IndexPosition),
    Range((IndexPosition, IndexPosition)),
}

#[derive(Clone, Copy)]
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
    pub fn new(name: String) -> Result<Self> {
        let log_path = Path::new(LOG_PATH).join(format!("{}.log", &name));
        let idx_path = Path::new(LOG_PATH).join(format!("{}.index", &name));
        let log_file = File::options().read(true).write(true).open(log_path)?;
        let index_file = File::options().read(true).write(true).open(idx_path)?;
        Ok(Self {
            log_file,
            index_file,
            prev_offset: 0,
            last_offset: 0,
            active: true,
        })
    }

    pub fn append_record(&mut self, record: record::Record) -> Result<()> {
        let mut writer = BufWriter::new(&self.log_file);
        record.write(&mut writer)?;
        self.last_offset += 1;
        if self.last_offset - self.prev_offset >= OFFSET_THRESHOLD {
            let offset = writer.seek(SeekFrom::Current(0))?;
            println!("OFFSET: {} - POSITION: {}", self.last_offset, offset);
            let mut idx_writer = BufWriter::new(&self.index_file);
            let index_position = IndexPosition::new(self.last_offset as u32, offset as u32);
            index_position.write(&mut idx_writer)?;
            self.prev_offset = self.last_offset;
        }
        Ok(())
    }

    pub fn read_at(&mut self, offset: u64) -> Result<record::Record> {
        match self.read_index(offset as u32) {
            Ok(SearchResult::Single(index_position)) => {
                let buffer_size = index_position.position as usize;
                let mut buf = vec![0u8; buffer_size];
                self.log_file
                    .seek(SeekFrom::Start(index_position.position as u64))?;
                self.log_file
                    .read_exact(&mut buf)
                    .expect("Error reading log file");
                let mut reader = BufReader::new(&buf[..]);
                let r = record::Record::from_binary(&mut reader)?;
                Ok(r)
            }
            Ok(SearchResult::Range((index_position, next_position))) => {
                let buffer_size = (next_position.position - index_position.position) as usize;
                let offset_count = next_position.offset - index_position.offset;
                let mut buf = vec![0u8; buffer_size];
                self.log_file
                    .seek(SeekFrom::Start(index_position.position as u64))?;
                self.log_file
                    .read_exact(&mut buf)
                    .expect("Error reading log file");
                let mut records: Vec<record::Record> = Vec::new();
                let mut pointer = 0;
                let mut reader = BufReader::new(&buf[..]);
                while pointer < offset_count {
                    let r = record::Record::from_binary(&mut reader)?;
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
        let mmap = unsafe { MmapOptions::new().map(&self.index_file)? };
        let positions: Vec<IndexPosition> = mmap
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
                let index_position = &positions[0];
                Ok(SearchResult::Single(*index_position))
            }
            Err(off) => {
                let index_position = &positions[off - 1];
                let next_position = &positions[off];
                Ok(SearchResult::Range((*index_position, *next_position)))
            }
        }
    }
}
