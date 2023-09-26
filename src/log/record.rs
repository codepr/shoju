use byteorder::{NetworkEndian, ReadBytesExt, WriteBytesExt};
use chrono::{DateTime, NaiveDateTime, Utc};
use std::fmt;
use std::io::{self, Read, Write};
use std::mem::size_of;

#[derive(Clone, Debug, PartialEq)]
pub struct Record {
    pub offset: u64,
    timestamp: u128,
    value: Vec<u8>,
}

impl fmt::Display for Record {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let ts_secs = self.timestamp / 1000;
        let ts_ns = (self.timestamp % 1000) * 1_000_000;
        let dt = DateTime::<Utc>::from_naive_utc_and_offset(
            NaiveDateTime::from_timestamp(ts_secs.try_into().unwrap(), ts_ns as u32),
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
    pub fn new(offset: u64, value: Vec<u8>) -> Record {
        Self {
            offset,
            timestamp: std::time::UNIX_EPOCH.elapsed().unwrap().as_millis(),
            value,
        }
    }

    pub fn write(&self, buf: &mut impl Write) -> io::Result<usize> {
        buf.write_u64::<NetworkEndian>(self.offset)?;
        buf.write_u128::<NetworkEndian>(self.timestamp)?;
        buf.write_u32::<NetworkEndian>(self.value.len() as u32)?;
        buf.write_all(&self.value)?;
        Ok(size_of::<u64>() + size_of::<u128>() + size_of::<u32>() + self.value.len())
    }

    pub fn from_binary(buf: &mut impl Read) -> io::Result<Self> {
        let offset = buf.read_u64::<NetworkEndian>()?;
        let timestamp = buf.read_u128::<NetworkEndian>()?;
        let value_size = buf.read_u32::<NetworkEndian>()?;
        let mut payload_binary = vec![0u8; value_size as usize];
        buf.read_exact(&mut payload_binary)?;
        Ok(Self {
            offset,
            timestamp,
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
        let record = Record::new(0, "test_value".into());
        assert_eq!(record.offset, 0);
        assert_eq!(
            record.value,
            &[116, 101, 115, 116, 95, 118, 97, 108, 117, 101]
        );
    }

    #[test]
    fn test_write() {
        let record = Record::new(0, "test_value".into());
        let mut buffer = vec![];
        record.write(&mut buffer).unwrap();
        let mut reader = BufReader::new(&buffer[..]);
        let expected = Record::from_binary(&mut reader).unwrap();
        assert_eq!(record, expected,);
    }
}
