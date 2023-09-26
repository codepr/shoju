use crate::log::record::Record;
use crate::log::segment::Segment;
use crate::log::LOG_PATH;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::fs;
use std::io::Result;
use std::path::Path;

const SIZE_THRESHOLD: usize = 524288;

pub struct Partition {
    sealed_segments: Vec<Segment>,
    active_segment: Segment,
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

        paths.sort();

        // let active_segment_log = paths.into_iter().last().unwrap();
        let head = &paths[0];
        let head_start_offset = head.parse::<u64>().expect("Log file name not compliant");
        let tail = &paths[1..];
        let sealed_segments: Vec<Segment> = tail
            .into_iter()
            .map(|name| {
                let starting_offset = name.parse::<u64>().expect("Log file name not compliant");
                Segment::from_disk(LOG_PATH, false, starting_offset).unwrap()
            })
            .collect();
        Ok(Partition {
            sealed_segments,
            active_segment: Segment::from_disk(LOG_PATH, true, head_start_offset)?,
        })
    }

    pub fn append_record(&mut self, value: &[u8]) -> Result<()> {
        if self.active_segment.size() >= SIZE_THRESHOLD {
            let last_offset = self.active_segment.last_offset;
            let new_segment = Segment::new(LOG_PATH, true, last_offset + 1)?;
            self.active_segment.seal();
            self.sealed_segments.push(self.active_segment.clone());
            self.active_segment = new_segment;
        }
        self.active_segment.append_record(value)
    }

    pub fn find_record(&mut self, offset: u64) -> Result<Record> {
        if offset <= self.active_segment.starting_offset {
            self.active_segment.read_at(offset)
        } else {
            match self
                .sealed_segments
                .binary_search_by(|probe| probe.starting_offset.cmp(&offset).then(Ordering::Less))
            {
                Ok(i) => self.sealed_segments[i].read_at(offset),
                Err(0) => {
                    if self.sealed_segments.len() == 0 {
                        self.active_segment.read_at(offset)
                    } else {
                        self.sealed_segments[0].read_at(offset)
                    }
                }
                Err(n) => self.sealed_segments[n - 1].read_at(offset),
            }
        }
    }
}
