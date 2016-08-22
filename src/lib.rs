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
        let offset = try!(self.read_offset(&txn));
        let key = try!(encode_key(offset));
        try!(txn.bind(&self.data).insert(&(&key as &[u8]), &data));
        debug!("Inserted {:?}bytes", data.len());
        try!(self.write_offset(&txn, offset + 1));
        try!(txn.commit());

        Ok(())
    }

    fn read_offset(&self, txn: &Transaction) -> Result<u64> {
        let val = {
            let val = match txn.bind(&self.meta).get(&WRITER_NEXT) {
                Ok(val) => val,
                Err(lmdb_rs::MdbError::NotFound) => 0,
                Err(e) => return Err(e.into()),
            };
            val
        };

        debug!("Producer at pos: {:?}", val);
        Ok(val)
    }
    fn write_offset(&self, txn: &Transaction, off: u64) -> Result<()> {
        try!(txn.bind(&self.meta).set(&WRITER_NEXT, &off));
        debug!("Producer now at pos {:?}", off);
        Ok(())
    }
}

pub struct Consumer {
    env: lmdb_rs::Environment,
    offset: u64,
}

impl Consumer {
    pub fn new<P: AsRef<Path>>(place: P) -> Result<Self> {
        let db = try!(EnvBuilder::new().max_dbs(2).open(place.as_ref(), 0o777));
        Ok(Consumer {
            env: db,
            offset: 0,
        })
    }

    pub fn poll(&mut self) -> Result<Option<Vec<u8>>> {
        let db_handle = try!(self.env.create_db(DATA, DbFlags::empty()));

        let key = try!(encode_key(self.offset));

        let val = {
            let txn = try!(self.env.get_reader());
            let val = match txn.bind(&db_handle).get(&(&key as &[u8])) {
                Ok(val) => Some(val),
                Err(lmdb_rs::MdbError::NotFound) => None,
                Err(e) => return Err(e.into()),
            };
            val
        };
        self.offset += 1;

        Ok(val)
    }
}
