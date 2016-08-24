extern crate lmdb;
extern crate byteorder;
#[macro_use]
extern crate error_chain;
#[macro_use]
extern crate log;
use std::path::Path;
use std::io::Cursor;
use byteorder::{BigEndian, WriteBytesExt, ReadBytesExt};

use lmdb::{DatabaseFlags, Database, Environment, RwTransaction, Transaction, WriteFlags};

mod errors;

use errors::*;

const PRODUCER_OFFSETS: &'static str = "prod";
const CONSUMER_OFFSETS: &'static str = "cons";
const DATA: &'static str = "data";
// 1TGB. That'll be enough, right?
const ARBITARILY_LARGE: usize = 1 << 40;

const WRITER_NEXT: &'static str = "writer-next";

pub struct Producer {
    env: Environment,
    meta: Database,
    data: Database,
}

fn encode_key(val: u64) -> Result<[u8; 8]> {
    let mut key = [0u8; 8];
    {
        let mut wr = Cursor::new(&mut key as &mut [u8]);
        try!(wr.write_u64::<BigEndian>(val));
    }

    Ok(key)
}

fn decode_key(val: &[u8]) -> Result<u64> {
    let mut r = Cursor::new(val);
    Ok(try!(r.read_u64::<BigEndian>()))
}



fn read_offset(meta: Database, txn: &RwTransaction, key: &str) -> Result<u64> {
    let val = {
        let val = match txn.get(meta, &key) {
            Ok(val) => try!(decode_key(val)),
            Err(lmdb::Error::NotFound) => 0,
            Err(e) => return Err(e.into()),
        };
        val
    };

    trace!("{:?} at pos: {:?}", key, val);
    Ok(val)
}

fn write_offset(meta: Database, txn: &mut RwTransaction, key: &str, off: u64) -> Result<()> {
    let encoded = try!(encode_key(off));
    try!(txn.put(meta, &key, &encoded, WriteFlags::empty()));
    trace!("{:?} now at pos {:?}", key, off);
    Ok(())
}

impl Producer {
    pub fn new<P: AsRef<Path>>(place: P) -> Result<Self> {
        debug!("Producer Open env at: {:?}", place.as_ref());
        let env = try!(Environment::new()
                           .set_max_dbs(3)
                           .set_map_size(ARBITARILY_LARGE)
                           .open(place.as_ref()));
        let meta = try!(env.create_db(Some(PRODUCER_OFFSETS), DatabaseFlags::empty()));
        let data = try!(env.create_db(Some(DATA), DatabaseFlags::empty()));
        Ok(Producer {
            env: env,
            meta: meta,
            data: data,
        })
    }

    pub fn produce(&mut self, data: &[u8]) -> Result<()> {
        let mut txn = try!(self.env.begin_rw_txn());
        let offset = try!(read_offset(self.meta, &txn, WRITER_NEXT));
        let key = try!(encode_key(offset));
        try!(txn.put(self.data, &key, &data, lmdb::NO_OVERWRITE));
        trace!("wrote: {:?}", data);
        try!(write_offset(self.meta, &mut txn, WRITER_NEXT, offset + 1));
        try!(txn.commit());

        Ok(())
    }
}

pub struct Consumer {
    env: lmdb::Environment,
    meta: Database,
    data: Database,
    name: String,
}

#[derive(Debug,Clone,Eq,PartialEq)]
pub struct Entry {
    off: u64,
    pub data: Vec<u8>,
}

impl Consumer {
    pub fn new<P: AsRef<Path>>(place: P, name: &str) -> Result<Self> {
        let env = try!(Environment::new()
                           .set_max_dbs(3)
                           .set_map_size(ARBITARILY_LARGE)
                           .open(place.as_ref()));
        let meta = try!(env.create_db(Some(CONSUMER_OFFSETS), DatabaseFlags::empty()));
        let data = try!(env.create_db(Some(DATA), DatabaseFlags::empty()));
        Ok(Consumer {
            env: env,
            meta: meta,
            data: data,
            name: name.to_string(),
        })
    }

    pub fn poll(&mut self) -> Result<Option<Entry>> {
        let mut txn = try!(self.env.begin_rw_txn());
        let offset = try!(read_offset(self.meta, &txn, &self.name));
        let key = try!(encode_key(offset));
        let val = {
            let val = match txn.get(self.data, &key) {
                Ok(val) => {
                    Some(Entry {
                        off: offset,
                        data: val.to_vec(),
                    })
                }
                Err(lmdb::Error::NotFound) => None,
                Err(e) => return Err(e.into()),
            };
            val
        };
        trace!("read @{:?}: {:?}", offset, val);
        try!(write_offset(self.meta, &mut txn, &self.name, offset + 1));
        try!(txn.commit());

        Ok(val)
    }
}
