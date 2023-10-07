//! An event record, represents an unique information in a precise point in time
//!
//! A `Record` is formed by an offset, a timestamp and the content information
//! defining the event. An event can be appended to a segment and persisted in a log file. It's
//! the smallest abstractiion in the system.
use byteorder::{NetworkEndian, ReadBytesExt, WriteBytesExt};
use chrono::{DateTime, NaiveDateTime, Utc};
use std::fmt;
use std::io::{self, Read, Write};
use std::mem::size_of;

#[derive(Clone, Debug, PartialEq)]
pub struct Record {
    pub offset: u64,
    pub timestamp: u128,
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
}

impl fmt::Display for Record {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let ts_secs = self.timestamp / 1000;
        let ts_ns = (self.timestamp % 1000) * 1_000_000;
        let dt = DateTime::<Utc>::from_naive_utc_and_offset(
            NaiveDateTime::from_timestamp_opt(ts_secs.try_into().unwrap(), ts_ns as u32).unwrap(),
            Utc,
        );
        write!(
            f,
            "{} - offset: {} ({} bytes)",
            dt,
            self.offset,
            self.value.len()
        )
    }
}

impl Record {
    pub fn new(offset: u64, key: Option<Vec<u8>>, value: Vec<u8>) -> Record {
        Self {
            offset,
            timestamp: std::time::UNIX_EPOCH.elapsed().unwrap().as_millis(),
            key,
            value,
        }
    }

    pub fn binary_size(&self) -> usize {
        size_of::<u64>()
            + size_of::<u128>()
            + size_of::<u32>()
            + self.value.len()
            + size_of::<u32>()
            + self.key.as_ref().map_or(0, |k| k.len())
    }

    pub fn write(&self, buf: &mut impl Write) -> io::Result<usize> {
        buf.write_u64::<NetworkEndian>(self.offset)?;
        buf.write_u128::<NetworkEndian>(self.timestamp)?;
        match &self.key {
            Some(k) => {
                buf.write_u32::<NetworkEndian>(k.len() as u32)?;
                buf.write_all(&k)?;
            }
            None => buf.write_u32::<NetworkEndian>(0)?,
        };
        buf.write_u32::<NetworkEndian>(self.value.len() as u32)?;
        buf.write_all(&self.value)?;
        Ok(self.binary_size())
    }

    pub fn from_binary(buf: &mut impl Read) -> io::Result<Self> {
        let offset = buf.read_u64::<NetworkEndian>()?;
        let timestamp = buf.read_u128::<NetworkEndian>()?;
        let key_size = buf.read_u32::<NetworkEndian>()?;
        let key_binary = if key_size > 0 {
            let mut key_b = vec![0u8; key_size as usize];
            buf.read_exact(&mut key_b)?;
            Some(key_b)
        } else {
            None
        };
        let value_size = buf.read_u32::<NetworkEndian>()?;
        let mut payload_binary = vec![0u8; value_size as usize];
        buf.read_exact(&mut payload_binary)?;
        Ok(Self {
            offset,
            timestamp,
            key: key_binary,
            value: payload_binary,
        })
    }
}

#[cfg(test)]
mod record_tests {
    use super::*;
    use std::io::BufReader;

    #[test]
    fn test_new() {
        let record = Record::new(0, Some("test_key".into()), "test_value".into());
        assert_eq!(record.offset, 0);
        assert_eq!(
            record.value,
            &[116, 101, 115, 116, 95, 118, 97, 108, 117, 101]
        );
    }

    #[test]
    fn test_binary_size() {
        let record = Record::new(0, Some("test_key".into()), "test_value".into());
        assert_eq!(record.binary_size(), 50);
    }

    #[test]
    fn test_write() {
        let record = Record::new(0, Some("test_key".into()), "test_value".into());
        let mut buffer = vec![];
        record.write(&mut buffer).unwrap();
        let mut reader = BufReader::new(&buffer[..]);
        let expected = Record::from_binary(&mut reader).unwrap();
        assert_eq!(record, expected,);
    }
}
