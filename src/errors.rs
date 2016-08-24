use lmdb;
use std;

error_chain!(
    foreign_links {
        lmdb::Error, Mdb;
        std::io::Error, Io;
    }
);
