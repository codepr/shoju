use byteorder::{NetworkEndian, ReadBytesExt, WriteBytesExt};
use memmap2::MmapOptions;
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

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct OffsetRange {
    pub begin: Position,
    pub end: Position,
}

impl OffsetRange {
    pub fn new(begin: Position, end: Position) -> Self {
        Self { begin, end }
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

    pub fn find_offset(&self, offset: u32) -> Result<OffsetRange> {
        if self.size == 0 {
            return Ok(OffsetRange::new(Position::new(0, 0), Position::new(0, 0)));
        }
        let relative_offset = (offset as u64 - self.base_offset) as u32;
        let starting_offset =
            ((relative_offset as usize / self.offset_interval) * ENTRY_SIZE) as usize;
        let starting_offset = if starting_offset == 0 {
            starting_offset
        } else {
            starting_offset - ENTRY_SIZE
        };
        let end_offset = if self.size >= (starting_offset + (ENTRY_SIZE * 2)) {
            starting_offset + (ENTRY_SIZE * 2)
        } else {
            self.size
        };

        let mmap = unsafe { MmapOptions::new().map(&self.file)? };
        let positions: Vec<Position> = mmap[starting_offset..end_offset]
            .chunks(ENTRY_SIZE)
            .map(|mut c| Position::from_binary(&mut c).unwrap())
            .collect();

        if offset < positions[0].relative_offset {
            Ok(OffsetRange::new(Position::new(0, 0), positions[0]))
        } else {
            if positions.len() > 1 {
                Ok(OffsetRange::new(positions[0], positions[1]))
            } else {
                Ok(OffsetRange::new(positions[0], positions[0].clone()))
            }
        }
    }
}

#[cfg(test)]
mod position_tests {
    use super::Position;
    use std::io::BufReader;

    #[test]
    fn test_new() {
        let idx_position = Position::new(0, 0);
        assert_eq!(
            idx_position,
            Position {
                relative_offset: 0,
                position: 0
            }
        );
    }

    #[test]
    fn test_write() {
        let idx_position = Position::new(0, 0);
        let mut buffer = vec![];
        idx_position.write(&mut buffer).unwrap();
        let mut reader = BufReader::new(&buffer[..]);
        let expected = Position::from_binary(&mut reader).unwrap();
        assert_eq!(idx_position, expected,);
    }
}

#[cfg(test)]
mod index_tests {

    use super::{Index, OffsetRange, Position, ENTRY_SIZE};
    use std::fs;
    use std::path::Path;
    use tempdir::TempDir;

    #[test]
    fn test_new() {
        let tmp_dir = TempDir::new("test_tempdir").unwrap();
        let expected_file = tmp_dir.path().join("00000000000000000000.index");

        let index = Index::new(&tmp_dir.path().to_path_buf(), 0, 10).unwrap();

        assert!(expected_file.as_path().exists());
        assert_eq!(index.base_offset, 0);
        assert_eq!(index.offset_interval, 10);
        assert_eq!(index.size, 0);
        tmp_dir.close().unwrap();
    }

    #[test]
    fn test_load_from_disk() {
        let tmp_dir = TempDir::new("test_tempdir").unwrap();
        let expected_file = tmp_dir.path().join("00000000000000000048.index");
        fs::File::create(&expected_file).unwrap();

        let index = Index::load_from_disk(&tmp_dir.path().to_path_buf(), 48, 10).unwrap();

        assert!(expected_file.as_path().exists());
        assert_eq!(index.base_offset, 48);
        assert_eq!(index.offset_interval, 10);
        assert_eq!(index.size, 0);
        tmp_dir.close().unwrap();
    }

    #[test]
    #[should_panic]
    fn test_invalid_load_from_disk() {
        Index::new(&Path::new("dont-exist-dir").to_path_buf(), 0, 10).unwrap();
    }

    #[test]
    fn test_append_position() {
        let tmp_dir = TempDir::new("test_tempdir").unwrap();
        let expected_file = tmp_dir.path().join("00000000000000000000.index");
        fs::File::create(&expected_file).unwrap();

        let mut index = Index::new(&tmp_dir.path().to_path_buf(), 0, 12).unwrap();

        index.append_position(12, 400).unwrap();

        assert_eq!(index.size, ENTRY_SIZE);

        assert_eq!(
            fs::read(expected_file).unwrap(),
            &[0, 0, 0, 12, 0, 0, 1, 144]
        );

        index.append_position(24, 1011).unwrap();
        assert_eq!(index.size, ENTRY_SIZE * 2);
        tmp_dir.close().unwrap();
    }

    #[test]
    fn test_find_offset() {
        let tmp_dir = TempDir::new("test_tempdir").unwrap();
        let expected_file = tmp_dir.path().join("00000000000000000000.index");
        fs::File::create(&expected_file).unwrap();

        let mut index = Index::new(&tmp_dir.path().to_path_buf(), 0, 20).unwrap();

        assert_eq!(
            index.find_offset(0).unwrap(),
            OffsetRange {
                begin: Position {
                    relative_offset: 0,
                    position: 0
                },
                end: Position {
                    relative_offset: 0,
                    position: 0
                }
            }
        );

        assert_eq!(
            index.find_offset(16).unwrap(),
            OffsetRange {
                begin: Position {
                    relative_offset: 0,
                    position: 0
                },
                end: Position {
                    relative_offset: 0,
                    position: 0
                }
            }
        );

        index.append_position(20, 150).unwrap();
        index.append_position(40, 406).unwrap();

        assert_eq!(
            index.find_offset(0).unwrap(),
            OffsetRange {
                begin: Position {
                    relative_offset: 0,
                    position: 0
                },
                end: Position {
                    relative_offset: 20,
                    position: 150
                }
            }
        );

        assert_eq!(
            index.find_offset(16).unwrap(),
            OffsetRange {
                begin: Position {
                    relative_offset: 0,
                    position: 0
                },
                end: Position {
                    relative_offset: 20,
                    position: 150
                }
            }
        );

        assert_eq!(
            index.find_offset(27).unwrap(),
            OffsetRange {
                begin: Position {
                    relative_offset: 20,
                    position: 150
                },
                end: Position {
                    relative_offset: 40,
                    position: 406
                }
            }
        );
        assert_eq!(
            index.find_offset(40).unwrap(),
            OffsetRange {
                begin: Position {
                    relative_offset: 40,
                    position: 406
                },
                end: Position {
                    relative_offset: 40,
                    position: 406
                }
            }
        );
        tmp_dir.close().unwrap();
    }
}
