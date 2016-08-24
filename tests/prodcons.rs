extern crate lmqueue;
extern crate tempdir;
extern crate env_logger;

#[test]
fn can_produce_none() {
    env_logger::init().unwrap_or(());
    let dir = tempdir::TempDir::new("store").expect("store-dir");
    let mut cons = lmqueue::Consumer::new(dir.path(), "default").expect("consumer");
    assert_eq!(cons.poll().expect("poll").map(|e| e.data), None)
}

#[test]
fn can_produce_one() {
    env_logger::init().unwrap_or(());
    let dir = tempdir::TempDir::new("store").expect("store-dir");
    let mut prod = lmqueue::Producer::new(dir.path()).expect("producer");
    let mut cons = lmqueue::Consumer::new(dir.path(), "default").expect("consumer");
    prod.produce(b"42").expect("produce");

    assert_eq!(cons.poll().expect("poll").map(|e| e.data), Some(b"42".to_vec()));
    assert_eq!(cons.poll().expect("poll").map(|e| e.data), None)
}

#[test]
fn can_produce_several() {
    env_logger::init().unwrap_or(());
    let dir = tempdir::TempDir::new("store").expect("store-dir");
    let mut prod = lmqueue::Producer::new(dir.path()).expect("producer");
    let mut cons = lmqueue::Consumer::new(dir.path(), "default").expect("consumer");
    prod.produce(b"0").expect("produce");
    prod.produce(b"1").expect("produce");
    prod.produce(b"2").expect("produce");

    assert_eq!(cons.poll().expect("poll").map(|e| e.data), Some(b"0".to_vec()));
    assert_eq!(cons.poll().expect("poll").map(|e| e.data), Some(b"1".to_vec()));
    assert_eq!(cons.poll().expect("poll").map(|e| e.data), Some(b"2".to_vec()));
    assert_eq!(cons.poll().expect("poll").map(|e| e.data), None)
}

#[test]
fn can_produce_incrementally() {
    env_logger::init().unwrap_or(());
    let dir = tempdir::TempDir::new("store").expect("store-dir");
    lmqueue::Producer::new(dir.path()).expect("producer").produce(b"0").expect("produce");
    lmqueue::Producer::new(dir.path()).expect("producer").produce(b"1").expect("produce");
    lmqueue::Producer::new(dir.path()).expect("producer").produce(b"2").expect("produce");

    let mut cons = lmqueue::Consumer::new(dir.path(), "default").expect("consumer");
    assert_eq!(cons.poll().expect("poll").map(|e| e.data), Some(b"0".to_vec()));
    assert_eq!(cons.poll().expect("poll").map(|e| e.data), Some(b"1".to_vec()));
    assert_eq!(cons.poll().expect("poll").map(|e| e.data), Some(b"2".to_vec()));
    assert_eq!(cons.poll().expect("poll").map(|e| e.data), None)
}

#[test]
fn can_consume_incrementally() {
    env_logger::init().unwrap_or(());
    let dir = tempdir::TempDir::new("store").expect("store-dir");
    let mut prod = lmqueue::Producer::new(dir.path()).expect("producer");
    prod.produce(b"0").expect("produce");
    prod.produce(b"1").expect("produce");
    prod.produce(b"2").expect("produce");

    for expected in &[Some(b"0"), Some(b"1"), Some(b"2"), None] {
        let mut cons = lmqueue::Consumer::new(dir.path(), "default").expect("consumer");
        let entry =cons.poll().expect("poll");
        if let Some(ref e) = entry {
            cons.commit_upto(&e).expect("commit");
        }
        assert_eq!(entry.map(|e| e.data), expected.map(|v| v.to_vec()));
    }
}

#[test]
fn can_restart_consume_at_commit_point() {
    env_logger::init().unwrap_or(());
    let dir = tempdir::TempDir::new("store").expect("store-dir");
    let mut prod = lmqueue::Producer::new(dir.path()).expect("producer");
    prod.produce(b"0").expect("produce");
    prod.produce(b"1").expect("produce");
    prod.produce(b"2").expect("produce");

    {
        let mut cons = lmqueue::Consumer::new(dir.path(), "default").expect("consumer");
        let entry = cons.poll().expect("poll");
        cons.commit_upto(entry.as_ref().unwrap()).expect("commit");
        assert_eq!(entry.map(|e| e.data), Some(b"0".to_vec()));
    }

    {
        let mut cons = lmqueue::Consumer::new(dir.path(), "default").expect("consumer");
        let entry = cons.poll().expect("poll");
        assert_eq!(entry.map(|e| e.data), Some(b"1".to_vec()));
    }
    {
        let mut cons = lmqueue::Consumer::new(dir.path(), "default").expect("consumer");
        let entry = cons.poll().expect("poll");
        assert_eq!(entry.map(|e| e.data), Some(b"1".to_vec()));
    }
}

#[test]
fn can_progress_without_commit() {
    env_logger::init().unwrap_or(());
    let dir = tempdir::TempDir::new("store").expect("store-dir");
    let mut prod = lmqueue::Producer::new(dir.path()).expect("producer");
    prod.produce(b"0").expect("produce");
    prod.produce(b"1").expect("produce");
    prod.produce(b"2").expect("produce");

    {
        let mut cons = lmqueue::Consumer::new(dir.path(), "default").expect("consumer");
        let entry = cons.poll().expect("poll");
        assert_eq!(entry.map(|e| e.data), Some(b"0".to_vec()));
        let entry = cons.poll().expect("poll");
        assert_eq!(entry.map(|e| e.data), Some(b"1".to_vec()));
    }
}

#[test]
fn can_consume_multiply() {
    env_logger::init().unwrap_or(());
    let dir = tempdir::TempDir::new("store").expect("store-dir");
    let mut prod = lmqueue::Producer::new(dir.path()).expect("producer");
    prod.produce(b"0").expect("produce");
    prod.produce(b"1").expect("produce");

    {
        let mut cons = lmqueue::Consumer::new(dir.path(), "one").expect("consumer");
        let entry =cons.poll().expect("poll");
        if let Some(ref e) = entry {
            cons.commit_upto(&e).expect("commit");
        }
        assert_eq!(entry.map(|e| e.data), Some(b"0".to_vec()));
    }
    {
        let mut cons = lmqueue::Consumer::new(dir.path(), "one").expect("consumer");
        let entry =cons.poll().expect("poll");
        assert_eq!(entry.map(|e| e.data), Some(b"1".to_vec()));
    }
    {
        let mut cons = lmqueue::Consumer::new(dir.path(), "two").expect("consumer");
        let entry =cons.poll().expect("poll");
        if let Some(ref e) = entry {
            cons.commit_upto(&e).expect("commit");
        }
        assert_eq!(entry.map(|e| e.data), Some(b"0".to_vec()));
    }
    {
        let mut cons = lmqueue::Consumer::new(dir.path(), "two").expect("consumer");
        let entry =cons.poll().expect("poll");
        assert_eq!(entry.map(|e| e.data), Some(b"1".to_vec()));
    }
}
