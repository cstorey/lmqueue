extern crate lmdb_zero;
extern crate byteorder;
#[macro_use]
extern crate error_chain;
#[macro_use]
extern crate log;
use std::path::Path;
use std::io::Cursor;
use byteorder::{BigEndian, WriteBytesExt, ReadBytesExt};

use lmdb_zero::{Environment, EnvBuilder, Database, ConstAccessor, ReadTransaction, WriteAccessor,
                WriteTransaction, put, open, error};

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



fn read_offset(meta: &Database, txn: &ConstAccessor, key: &str) -> Result<u64> {
    let val = {
        let val = match txn.get(meta, key) {
            Ok(val) => try!(decode_key(val)),
            Err(e) if e.code == error::NOTFOUND => 0,
            Err(e) => return Err(e.into()),
        };
        val
    };

    trace!("{:?} at pos: {:?}", key, val);
    Ok(val)
}

fn write_offset(meta: &Database, txn: &mut WriteAccessor, key: &str, off: u64) -> Result<()> {
    let encoded = try!(encode_key(off));
    try!(txn.put(meta, key, &encoded, put::Flags::empty()));
    trace!("{:?} now at pos {:?}", key, off);
    Ok(())
}

impl Producer {
    pub fn new<P: AsRef<str>>(place: P) -> Result<Self> {
        debug!("Producer Open env at: {:?}", place.as_ref());
        let mut b = try!(EnvBuilder::new());
        try!(b.set_maxdbs(3));
        try!(b.set_mapsize(ARBITARILY_LARGE));
        let env = unsafe { try!(b.open(place.as_ref(), open::Flags::empty(), 0o777)) };

        Ok(Producer { env: env })
    }
    fn meta(&self) -> Result<Database> {
        Ok(try!(open_db(&self.env, PRODUCER_OFFSETS)))
    }
    fn data(&self) -> Result<Database> {
        Ok(try!(open_db(&self.env, DATA)))
    }

    pub fn produce(&mut self, msg: &[u8]) -> Result<()> {
        let meta = try!(self.meta());
        let data = try!(self.data());
        let txn = try!(WriteTransaction::new(&self.env));
        {
            let mut acc = txn.access();
            let offset = try!(read_offset(&meta, &acc, WRITER_NEXT));
            let key = try!(encode_key(offset));
            try!(acc.put(&data, &key, msg, put::NOOVERWRITE));
            trace!("wrote: {:?}", msg);
            try!(write_offset(&meta, &mut acc, WRITER_NEXT, offset + 1));
            debug!("Produced at offset: {:?}", offset);
        }
        try!(txn.commit());

        Ok(())
    }
}

pub struct Consumer {
    env: Environment,
    name: String,
    offset: u64,
}

#[derive(Debug,Clone,Eq,PartialEq)]
pub struct Entry {
    offset: u64,
    pub data: Vec<u8>,
}


fn open_db<'a>(env: &'a Environment, name: &str) -> Result<Database<'a>> {
    let db = try!(Database::open(env,
                                 Some(name),
                                 &lmdb_zero::DatabaseOptions::new(lmdb_zero::db::CREATE)));
    Ok(db)
}
impl Consumer {
    pub fn new<P: AsRef<str>>(place: P, name: &str) -> Result<Self> {
        debug!("Consumer Open env at: {:?}", place.as_ref());
        let mut b = try!(EnvBuilder::new());
        try!(b.set_maxdbs(3));
        try!(b.set_mapsize(ARBITARILY_LARGE));
        let env = unsafe { try!(b.open(place.as_ref(), open::Flags::empty(), 0o777)) };
        let offset = {
            let meta = try!(open_db(&env, CONSUMER_OFFSETS));
            let txn = try!(ReadTransaction::new(&env));
            try!(read_offset(&meta, &txn.access(), name))
        };

        Ok(Consumer {
            env: env,
            name: name.to_string(),
            offset: offset,
        })
    }

    fn meta(&self) -> Result<Database> {
        Ok(try!(open_db(&self.env, CONSUMER_OFFSETS)))
    }
    fn data(&self) -> Result<Database> {
        Ok(try!(open_db(&self.env, DATA)))
    }

    pub fn poll(&mut self) -> Result<Option<Entry>> {
        let entry = {
            let data = try!(self.data());
            let txn = try!(ReadTransaction::new(&self.env));
            let key = try!(encode_key(self.offset));
            debug!("Attempt read at: {:?}", self.offset);
            match txn.access().get(&data, &key) {
                Ok(val) => {
                    let _: &[u8] = val;
                    Entry {
                        offset: self.offset,
                        data: val.to_vec(),
                    }
                }
                Err(e) if e.code == error::NOTFOUND => {
                    debug!("Nothing found");
                    return Ok(None);
                }
                Err(e) => return Err(e.into()),
            }
        };
        trace!("read {:?}", entry);

        self.offset += 1;
        Ok(Some(entry))
    }

    pub fn commit_upto(&self, entry: &Entry) -> Result<()> {
        let meta = try!(self.meta());
        let mut txn = try!(WriteTransaction::new(&self.env));
        try!(write_offset(&meta, &mut txn.access(), &self.name, entry.offset + 1));
        try!(txn.commit());
        Ok(())
    }
}
