pub mod index;
pub mod log;
mod pager;
pub mod record;
pub mod segment;

use record::Record;
use segment::Segment;
use segment::SegmentError;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::fs;
use std::io::Result;
use std::path::Path;

const LOG_PATH: &str = "logdir";
const LOG_MAX_SIZE: usize = 4096;
const OFFSET_INTERVAL: usize = 16;

pub struct Partition {
    segments: Vec<Segment>,
    active_segment_index: usize,
}

impl Partition {
    pub fn init() -> Result<Self> {
        let mut paths = fs::read_dir(LOG_PATH)?
            .into_iter()
            .flat_map(|f| f.map(|entry| entry.file_name()))
            .map(|name| {
                Path::new(&name)
                    .with_extension("")
                    .to_str()
                    .unwrap()
                    .to_owned()
            })
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();

        if paths.len() == 0 {
            Ok(Partition {
                segments: vec![Segment::new(LOG_PATH, 0, OFFSET_INTERVAL, true)?],
                active_segment_index: 0,
            })
        } else {
            paths.sort();
            let active_segment_index = paths.len();

            let segments: Vec<Segment> = paths
                .into_iter()
                .map(|name| {
                    let base_offset = name.parse::<u64>().expect("Log file name not compliant");
                    Segment::load_from_disk(LOG_PATH, base_offset, OFFSET_INTERVAL, false).unwrap()
                })
                .collect();
            Ok(Partition {
                segments,
                active_segment_index: active_segment_index - 1,
            })
        }
    }

    pub fn flush(&mut self) -> Result<()> {
        self.active_segment().flush()
    }

    pub fn append_record(&mut self, key: Option<Vec<u8>>, value: &[u8]) -> Result<()> {
        match self.active_segment().append_record(key.clone(), value) {
            Ok(()) => Ok(()),
            Err(SegmentError::FullSegment) => {
                match self.new_active_segment()?.append_record(key, value) {
                    Ok(()) => Ok(()),
                    Err(_) => panic!(),
                }
            }
            Err(SegmentError::Io(e)) => Err(e),
        }
    }

    pub fn find_record(&mut self, offset: u64) -> Result<Record> {
        match offset {
            v if v == self.active_segment().base_offset => self.active_segment().read_at(v),
            v if self.segments.len() > 0 && v < self.segments[0].base_offset => {
                self.active_segment().read_at(v)
            }
            v => {
                match self
                    .segments
                    .binary_search_by(|s| s.base_offset.cmp(&v).then(Ordering::Less))
                {
                    Ok(i) => self.segments[i].read_at(v),
                    Err(0) => {
                        if self.segments.len() == 0 {
                            self.active_segment().read_at(v)
                        } else {
                            self.segments[0].read_at(v)
                        }
                    }
                    Err(n) => self.segments[n - 1].read_at(v),
                }
            }
        }
    }

    fn active_segment(&mut self) -> &mut Segment {
        &mut self.segments[self.active_segment_index]
    }

    fn new_active_segment(&mut self) -> Result<&mut Segment> {
        let latest_offset = self.segments[self.active_segment_index].latest_offset();
        let new_segment = Segment::new(LOG_PATH, latest_offset, OFFSET_INTERVAL, true)?;
        self.segments[self.active_segment_index].seal();
        self.segments.push(new_segment);
        self.active_segment_index += 1;
        Ok(self.active_segment())
    }
}
