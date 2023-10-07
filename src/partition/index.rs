use byteorder::{NetworkEndian, ReadBytesExt, WriteBytesExt};
use memmap2::MmapOptions;
use std::cmp::Ordering;
use std::fs::{File, OpenOptions};
use std::io::BufWriter;
use std::io::{Read, Result, Write};
use std::path::PathBuf;

const ENTRY_SIZE: usize = 8;

#[derive(Debug)]
pub struct Index {
    file: File,
    size: usize,
    base_offset: u64,
    offset_interval: usize,
}

pub enum FindResult {
    Punctual(Position),
    Ahead((Position, Position)),
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Position {
    pub relative_offset: u32,
    pub position: u32,
}

impl Position {
    pub fn new(relative_offset: u32, position: u32) -> Self {
        Self {
            relative_offset,
            position,
        }
    }

    pub fn write(&self, buf: &mut impl Write) -> Result<()> {
        buf.write_u32::<NetworkEndian>(self.relative_offset)?;
        buf.write_u32::<NetworkEndian>(self.position)
    }

    pub fn from_binary(buf: &mut impl Read) -> Result<Self> {
        let relative_offset = buf.read_u32::<NetworkEndian>()?;
        let position = buf.read_u32::<NetworkEndian>()?;
        Ok(Self {
            relative_offset,
            position,
        })
    }
}

impl Index {
    pub fn new(path: &PathBuf, base_offset: u64, offset_interval: usize) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path.join(format!("{:020}.index", base_offset)))?;

        Ok(Self {
            file,
            size: 0,
            base_offset,
            offset_interval,
        })
    }

    pub fn load_from_disk(
        path: &PathBuf,
        base_offset: u64,
        offset_interval: usize,
    ) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .create(false)
            .append(true)
            .open(path.join(format!("{:020}.index", base_offset)))?;
        let size = file.metadata().unwrap().len();
        Ok(Self {
            file,
            size: size as usize,
            base_offset,
            offset_interval,
        })
    }

    pub fn flush(&mut self) -> Result<()> {
        self.file.flush()
    }

    pub fn append_position(&mut self, offset: u32, log_size: u32) -> Result<()> {
        let relative_offset = offset as u64 - self.base_offset;
        let new_row = Position::new(relative_offset as u32, log_size);
        let mut writer = BufWriter::new(&self.file);
        new_row.write(&mut writer)?;
        self.size += ENTRY_SIZE;
        Ok(())
    }

    pub fn find_offset(&self, offset: u32) -> Result<FindResult> {
        let relative_offset = (offset as u64 - self.base_offset) as u32;
        let starting_offset =
            ((relative_offset as usize / self.offset_interval) * ENTRY_SIZE) as usize;
        let starting_offset = if starting_offset == 0 {
            starting_offset
        } else {
            starting_offset - ENTRY_SIZE
        };
        let mmap = unsafe { MmapOptions::new().map(&self.file)? };
        let positions: Vec<Position> = mmap[starting_offset..]
            .chunks(8)
            .map(|mut c| Position::from_binary(&mut c).unwrap())
            .collect();

        let position = positions
            .binary_search_by(|p| p.relative_offset.cmp(&relative_offset).then(Ordering::Less));
        match position {
            Ok(pos) => {
                let index_position = &positions[pos];
                Ok(FindResult::Punctual(*index_position))
            }
            Err(0) => {
                let lower_offset = &positions[0];
                let higher_offset = &positions[if positions.len() > 0 { 1 } else { 0 }];
                Ok(FindResult::Ahead((
                    Position::new(
                        lower_offset.relative_offset - self.offset_interval as u32,
                        0,
                    ),
                    Position::new(
                        higher_offset.relative_offset - self.offset_interval as u32,
                        higher_offset.position,
                    ),
                )))
            }
            Err(off) => {
                let index_position = &positions[off - 1];
                if relative_offset % self.offset_interval as u32 == 0 || off == positions.len() {
                    Ok(FindResult::Punctual(*index_position))
                } else {
                    let next_position = &positions[off];
                    Ok(FindResult::Ahead((*index_position, *next_position)))
                }
            }
        }
    }
}
