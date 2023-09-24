use byteorder::{NetworkEndian, ReadBytesExt, WriteBytesExt};
use std::io::{self, Read, Write};

#[derive(Clone)]
pub struct Record {
    pub offset: u64,
    timestamp: u128,
    value: Vec<u8>,
}

impl Record {
    pub fn new(offset: u64, value: Vec<u8>) -> Record {
        Self {
            offset,
            timestamp: std::time::UNIX_EPOCH.elapsed().unwrap().as_millis(),
            value,
        }
    }

    pub fn write(&self, buf: &mut impl Write) -> io::Result<()> {
        buf.write_u64::<NetworkEndian>(self.offset)?;
        buf.write_u128::<NetworkEndian>(self.timestamp)?;
        buf.write_u32::<NetworkEndian>(self.value.len() as u32)?;
        buf.write_all(&self.value)
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
