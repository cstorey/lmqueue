use lmdb_rs;
use std;

error_chain!(
    foreign_links {
        lmdb_rs::MdbError, Mdb;
        std::io::Error, Io;
    }
);
