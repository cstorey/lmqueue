extern crate lmdb_zero;
extern crate byteorder;
#[macro_use]
extern crate error_chain;
#[macro_use]
extern crate log;
use std::path::Path;
use std::io::Cursor;
use std::iter;
use std::collections::BTreeMap;
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

#[derive(Debug)]
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
            // Move to next slot.
            let offset = try!(read_offset(&meta, &acc, WRITER_NEXT)) + 1;
            let key = try!(encode_key(offset));
            try!(acc.put(&data, &key, msg, put::NOOVERWRITE));
            trace!("wrote: {:?}", msg);
            try!(write_offset(&meta, &mut acc, WRITER_NEXT, offset));
            debug!("Produced at offset: {:?}", offset);
        }
        try!(txn.commit());

        Ok(())
    }
}

#[derive(Debug)]
pub struct Consumer {
    env: Environment,
    name: String,
    offset: u64,
}

#[derive(Debug,Clone,Eq,PartialEq)]
pub struct Entry {
    pub offset: u64,
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
            let next_offset = self.offset + 1;
            let key = try!(encode_key(next_offset));
            debug!("open cursor for {:?}", self);
            let mut cursor = try!(txn.cursor(&data).chain_err(|| "get cursor"));
            debug!("Attempt read from: {:?}", next_offset);
            match try!(mdb_maybe(cursor.seek_range_k::<[u8], [u8]>(&txn.access(), &key))) {
                Some((k, v)) => {
                    let off = try!(decode_key(k));
                    Entry {
                        offset: off,
                        data: v.to_vec(),
                    }
                }
                None => return Ok(None),
            }
        };
        trace!("read {:?}", entry);
        self.offset = entry.offset;

        Ok(Some(entry))
    }

    pub fn commit_upto(&self, entry: &Entry) -> Result<()> {
        let meta = try!(self.meta());
        let mut txn = try!(WriteTransaction::new(&self.env));
        try!(write_offset(&meta, &mut txn.access(), &self.name, entry.offset));
        try!(txn.commit());
        Ok(())
    }

    pub fn consumers(&self) -> Result<BTreeMap<String, u64>> {
        let db = try!(self.meta());
        // The transaction can be used for database created /before/ the txn,
        // so ensure we create the db before the txn. Otherwise, lmdb returns
        // the helpful `-EINVAL`.
        let txn = try!(ReadTransaction::new(&self.env));
        let mut ret = BTreeMap::new();

        debug!("open cursor for {:?}", self);
        let mut cursor = try!(txn.cursor(&db).chain_err(|| "get cursor"));
        let accessor = txn.access();
        let mut curr = try!(mdb_maybe(cursor.first(&accessor)));
        debug!("First: {:?}", curr);
        while let Some(kv) = curr {
            let (k, v): (&str, &[u8]) = kv;
            let offset = try!(decode_key(v));
            ret.insert(k.to_string(), offset);
            curr = try!(mdb_maybe(cursor.next(&accessor)));
            debug!("Next: {:?}", curr);
        }

        Ok(ret)
    }
}

fn mdb_maybe<T>(res: ::std::result::Result<T, lmdb_zero::Error>)
                -> ::std::result::Result<Option<T>, lmdb_zero::Error> {
    match res {
        Ok(kv) => Ok(Some(kv)),
        Err(e) if e.code == error::NOTFOUND => Ok(None),
        Err(e) => Err(e),
    }
}
