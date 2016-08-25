use lmdb_zero;
use std;

error_chain!(
    foreign_links {
        lmdb_zero::error::Error, Mdb;
        std::io::Error, Io;
    }
);
