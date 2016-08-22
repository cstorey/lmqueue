extern crate lmdb_rs;
extern crate byteorder;
#[macro_use]
extern crate error_chain;
#[macro_use]
extern crate log;
use std::path::Path;
use std::io::Cursor;
use byteorder::{BigEndian, WriteBytesExt};


use lmdb_rs::{DbFlags, DbHandle, EnvBuilder, Environment, Transaction};

mod errors;

use errors::*;

const META: &'static str = "meta";
const DATA: &'static str = "data";

const WRITER_NEXT: &'static str = "writer-next";
const CONSUMER_NEXT: &'static str = "consumer-next";

pub struct Producer {
    env: Environment,
    meta: DbHandle,
    data: DbHandle,
}

fn encode_key(val: u64) -> Result<[u8; 8]> {
    let mut key = [0u8; 8];
    {
        let mut wr = Cursor::new(&mut key as &mut [u8]);
        try!(wr.write_u64::<BigEndian>(val));
    }

    Ok(key)
}

fn read_offset(meta: &DbHandle, txn: &Transaction, key: &str) -> Result<u64> {
    let val = {
        let val = match txn.bind(meta).get(&key) {
            Ok(val) => val,
            Err(lmdb_rs::MdbError::NotFound) => 0,
            Err(e) => return Err(e.into()),
        };
        val
    };

    trace!("{:?} at pos: {:?}", key, val);
    Ok(val)
}
fn write_offset(meta: &DbHandle, txn: &Transaction, key: &str, off: u64) -> Result<()> {
    try!(txn.bind(meta).set(&key, &off));
    trace!("{:?} now at pos {:?}", key, off);
    Ok(())
}

impl Producer {
    pub fn new<P: AsRef<Path>>(place: P) -> Result<Self> {
        debug!("Producer Open env at: {:?}", place.as_ref());
        let env = try!(EnvBuilder::new().max_dbs(2).open(place.as_ref(), 0o777));
        let meta = try!(env.create_db(META, DbFlags::empty()));
        let data = try!(env.create_db(DATA, DbFlags::empty()));
        Ok(Producer {
            env: env,
            meta: meta,
            data: data,
        })
    }

    pub fn produce(&mut self, data: &[u8]) -> Result<()> {
        let txn = try!(self.env.new_transaction());
        let offset = try!(read_offset(&self.meta, &txn, WRITER_NEXT));
        let key = try!(encode_key(offset));
        try!(txn.bind(&self.data).insert(&(&key as &[u8]), &data));
        trace!("wrote: {:?}", data);
        try!(write_offset(&self.meta, &txn, WRITER_NEXT, offset + 1));
        try!(txn.commit());

        Ok(())
    }
}

pub struct Consumer {
    env: lmdb_rs::Environment,
    meta: DbHandle,
    data: DbHandle,
}

impl Consumer {
    pub fn new<P: AsRef<Path>>(place: P) -> Result<Self> {
        let env = try!(EnvBuilder::new().max_dbs(2).open(place.as_ref(), 0o777));
        let meta = try!(env.create_db(META, DbFlags::empty()));
        let data = try!(env.create_db(DATA, DbFlags::empty()));
        Ok(Consumer {
            env: env,
            meta: meta,
            data: data,
        })
    }

    pub fn poll(&mut self) -> Result<Option<Vec<u8>>> {
        let txn = try!(self.env.new_transaction());
        let offset = try!(read_offset(&self.meta, &txn, CONSUMER_NEXT));
        let key = try!(encode_key(offset));
        let val = {
            let val = match txn.bind(&self.data).get(&(&key as &[u8])) {
                Ok(val) => Some(val),
                Err(lmdb_rs::MdbError::NotFound) => None,
                Err(e) => return Err(e.into()),
            };
            val
        };
        trace!("read @{:?}: {:?}", offset, val);
        try!(write_offset(&self.meta, &txn, CONSUMER_NEXT, offset + 1));
        try!(txn.commit());

        Ok(val)
    }
}
