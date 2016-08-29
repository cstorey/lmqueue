extern crate lmqueue;
extern crate tempdir;
extern crate env_logger;

#[test]
fn can_produce_none() {
    env_logger::init().unwrap_or(());
    let dir = tempdir::TempDir::new("store").expect("store-dir");
    let mut cons = lmqueue::Consumer::new(dir.path().to_str().expect("path string"), "default").expect("consumer");
    assert_eq!(cons.poll().expect("poll").map(|e| e.data), None)
}

#[test]
fn can_produce_one() {
    env_logger::init().unwrap_or(());
    let dir = tempdir::TempDir::new("store").expect("store-dir");
    let mut prod = lmqueue::Producer::new(dir.path().to_str().expect("path string")).expect("producer");
    let mut cons = lmqueue::Consumer::new(dir.path().to_str().expect("path string"), "default").expect("consumer");
    prod.produce(b"42").expect("produce");

    assert_eq!(cons.poll().expect("poll").map(|e| e.data), Some(b"42".to_vec()));
    assert_eq!(cons.poll().expect("poll").map(|e| e.data), None)
}

#[test]
fn can_produce_several() {
    env_logger::init().unwrap_or(());
    let dir = tempdir::TempDir::new("store").expect("store-dir");
    let mut prod = lmqueue::Producer::new(dir.path().to_str().expect("path string")).expect("producer");
    let mut cons = lmqueue::Consumer::new(dir.path().to_str().expect("path string"), "default").expect("consumer");
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
    lmqueue::Producer::new(dir.path().to_str().expect("path string")).expect("producer").produce(b"0").expect("produce");
    lmqueue::Producer::new(dir.path().to_str().expect("path string")).expect("producer").produce(b"1").expect("produce");
    lmqueue::Producer::new(dir.path().to_str().expect("path string")).expect("producer").produce(b"2").expect("produce");

    let mut cons = lmqueue::Consumer::new(dir.path().to_str().expect("path string"), "default").expect("consumer");
    assert_eq!(cons.poll().expect("poll").map(|e| e.data), Some(b"0".to_vec()));
    assert_eq!(cons.poll().expect("poll").map(|e| e.data), Some(b"1".to_vec()));
    assert_eq!(cons.poll().expect("poll").map(|e| e.data), Some(b"2".to_vec()));
    assert_eq!(cons.poll().expect("poll").map(|e| e.data), None)
}

#[test]
fn can_consume_incrementally() {
    env_logger::init().unwrap_or(());
    let dir = tempdir::TempDir::new("store").expect("store-dir");
    let mut prod = lmqueue::Producer::new(dir.path().to_str().expect("path string")).expect("producer");
    prod.produce(b"0").expect("produce");
    prod.produce(b"1").expect("produce");
    prod.produce(b"2").expect("produce");

    for expected in &[Some(b"0"), Some(b"1"), Some(b"2"), None] {
        let mut cons = lmqueue::Consumer::new(dir.path().to_str().expect("path string"), "default").expect("consumer");
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
    let mut prod = lmqueue::Producer::new(dir.path().to_str().expect("path string")).expect("producer");
    prod.produce(b"0").expect("produce");
    prod.produce(b"1").expect("produce");
    prod.produce(b"2").expect("produce");

    {
        let mut cons = lmqueue::Consumer::new(dir.path().to_str().expect("path string"), "default").expect("consumer");
        let entry = cons.poll().expect("poll");
        cons.commit_upto(entry.as_ref().unwrap()).expect("commit");
        assert_eq!(entry.map(|e| e.data), Some(b"0".to_vec()));
    }

    {
        let mut cons = lmqueue::Consumer::new(dir.path().to_str().expect("path string"), "default").expect("consumer");
        let entry = cons.poll().expect("poll");
        assert_eq!(entry.map(|e| e.data), Some(b"1".to_vec()));
    }
    {
        let mut cons = lmqueue::Consumer::new(dir.path().to_str().expect("path string"), "default").expect("consumer");
        let entry = cons.poll().expect("poll");
        assert_eq!(entry.map(|e| e.data), Some(b"1".to_vec()));
    }
}

#[test]
fn can_progress_without_commit() {
    env_logger::init().unwrap_or(());
    let dir = tempdir::TempDir::new("store").expect("store-dir");
    let mut prod = lmqueue::Producer::new(dir.path().to_str().expect("path string")).expect("producer");
    prod.produce(b"0").expect("produce");
    prod.produce(b"1").expect("produce");
    prod.produce(b"2").expect("produce");

    {
        let mut cons = lmqueue::Consumer::new(dir.path().to_str().expect("path string"), "default").expect("consumer");
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
    let mut prod = lmqueue::Producer::new(dir.path().to_str().expect("path string")).expect("producer");
    prod.produce(b"0").expect("produce");
    prod.produce(b"1").expect("produce");

    {
        let mut cons = lmqueue::Consumer::new(dir.path().to_str().expect("path string"), "one").expect("consumer");
        let entry =cons.poll().expect("poll");
        if let Some(ref e) = entry {
            cons.commit_upto(&e).expect("commit");
        }
        assert_eq!(entry.map(|e| e.data), Some(b"0".to_vec()));
    }
    {
        let mut cons = lmqueue::Consumer::new(dir.path().to_str().expect("path string"), "one").expect("consumer");
        let entry =cons.poll().expect("poll");
        assert_eq!(entry.map(|e| e.data), Some(b"1".to_vec()));
    }
    {
        let mut cons = lmqueue::Consumer::new(dir.path().to_str().expect("path string"), "two").expect("consumer");
        let entry =cons.poll().expect("poll");
        if let Some(ref e) = entry {
            cons.commit_upto(&e).expect("commit");
        }
        assert_eq!(entry.map(|e| e.data), Some(b"0".to_vec()));
    }
    {
        let mut cons = lmqueue::Consumer::new(dir.path().to_str().expect("path string"), "two").expect("consumer");
        let entry =cons.poll().expect("poll");
        assert_eq!(entry.map(|e| e.data), Some(b"1".to_vec()));
    }
}

#[test]
fn can_list_zero_consumer_offsets() {
    env_logger::init().unwrap_or(());
    let dir = tempdir::TempDir::new("store").expect("store-dir");
    let mut prod = lmqueue::Producer::new(dir.path().to_str().expect("path string")).expect("producer");
    prod.produce(b"0").expect("produce");
    prod.produce(b"1").expect("produce");
    let cons = lmqueue::Consumer::new(dir.path().to_str().expect("path string"), "one").expect("consumer");
    let offsets = cons.consumers().expect("iter");
    assert!(offsets.is_empty());
}

#[test]
fn can_list_consumer_offset() {
    env_logger::init().unwrap_or(());
    let dir = tempdir::TempDir::new("store").expect("store-dir");
    let mut prod = lmqueue::Producer::new(dir.path().to_str().expect("path string")).expect("producer");
    prod.produce(b"0").expect("produce");
    prod.produce(b"1").expect("produce");

    let entry;
    {
        let mut cons = lmqueue::Consumer::new(dir.path().to_str().expect("path string"), "one").expect("consumer");
        entry =cons.poll().expect("poll").expect("some entry");
        cons.commit_upto(&entry).expect("commit");
    }

    let cons = lmqueue::Consumer::new(dir.path().to_str().expect("path string"), "one").expect("consumer");
    let offsets = cons.consumers().expect("iter");
    assert_eq!(offsets.len(), 1);
    assert_eq!(offsets.get("one"), Some(&entry.offset));
}


#[test]
fn can_list_consumer_offsets() {
    env_logger::init().unwrap_or(());
    let dir = tempdir::TempDir::new("store").expect("store-dir");
    let mut prod = lmqueue::Producer::new(dir.path().to_str().expect("path string")).expect("producer");
    prod.produce(b"0").expect("produce");
    prod.produce(b"1").expect("produce");

    let one;
    let two;
    {
        let mut cons = lmqueue::Consumer::new(dir.path().to_str().expect("path string"), "one").expect("consumer");
        let entry =cons.poll().expect("poll").expect("some entry");
        cons.commit_upto(&entry).expect("commit");
    }
    {
        let mut cons = lmqueue::Consumer::new(dir.path().to_str().expect("path string"), "one").expect("consumer");
        let entry =cons.poll().expect("poll").expect("some entry");
        cons.commit_upto(&entry).expect("commit");
        one = entry;
    }

    {
        let mut cons = lmqueue::Consumer::new(dir.path().to_str().expect("path string"), "two").expect("consumer");
        let entry =cons.poll().expect("poll").expect("some entry");
        cons.commit_upto(&entry).expect("commit");
        two = entry;
    }

    let cons = lmqueue::Consumer::new(dir.path().to_str().expect("path string"), "two").expect("consumer");
    let offsets = cons.consumers().expect("iter");
    assert_eq!(offsets.len(), 2);
    assert_eq!(offsets.get("one"), Some(&one.offset));
    assert_eq!(offsets.get("two"), Some(&two.offset));
}

#[test]
fn can_discard_queue() {
    env_logger::init().unwrap_or(());
    let dir = tempdir::TempDir::new("store").expect("store-dir");
    let mut prod = lmqueue::Producer::new(dir.path().to_str().expect("path string")).expect("producer");
    prod.produce(b"0").expect("produce");
    prod.produce(b"1").expect("produce");

    {
        let mut cons = lmqueue::Consumer::new(dir.path().to_str().expect("path string"), "one").expect("consumer");
        let entry =cons.poll().expect("poll").expect("some entry");
        cons.commit_upto(&entry).expect("commit");
    }

    {
        let cons = lmqueue::Consumer::new(dir.path().to_str().expect("path string"), "cleaner").expect("consumer");
        let one_off = cons.consumers().expect("consumers")["one"];
        cons.discard_upto(one_off).expect("discard");
    }

    {
        let mut cons = lmqueue::Consumer::new(dir.path().to_str().expect("path string"), "two").expect("consumer");
        let entry =cons.poll().expect("poll").expect("some entry");
        assert_eq!(&entry.data, b"1");
    }
}

#[test]
fn can_discard_on_empty() {
    env_logger::init().unwrap_or(());
    let dir = tempdir::TempDir::new("store").expect("store-dir");
    {
        let cons = lmqueue::Consumer::new(dir.path().to_str().expect("path string"), "cleaner").expect("consumer");
        cons.discard_upto(42).expect("discard");
    }
}

#[test]
fn can_discard_after_written() {
    env_logger::init().unwrap_or(());
    let dir = tempdir::TempDir::new("store").expect("store-dir");
    let mut prod = lmqueue::Producer::new(dir.path().to_str().expect("path string")).expect("producer");
    prod.produce(b"0").expect("produce");

    {
        let cons = lmqueue::Consumer::new(dir.path().to_str().expect("path string"), "cleaner").expect("consumer");
        cons.discard_upto(42).expect("discard");
    }
}

#[test]
fn can_remove_consumer_offset() {
    env_logger::init().unwrap_or(());
    let dir = tempdir::TempDir::new("store").expect("store-dir");
    let mut prod = lmqueue::Producer::new(dir.path().to_str().expect("path string")).expect("producer");
    prod.produce(b"0").expect("produce");
    prod.produce(b"1").expect("produce");
    prod.produce(b"2").expect("produce");

    {
        let mut cons = lmqueue::Consumer::new(dir.path().to_str().expect("path string"), "default").expect("consumer");
        let entry = cons.poll().expect("poll");
        cons.commit_upto(entry.as_ref().unwrap()).expect("commit");
    }

    {
        let mut cons = lmqueue::Consumer::new(dir.path().to_str().expect("path string"), "default").expect("consumer");
        let _ = cons.clear_offset().expect("clear_offset");
    }
    {
        let cons = lmqueue::Consumer::new(dir.path().to_str().expect("path string"), "default").expect("consumer");
        let consumers = cons.consumers().expect("consumers");
        assert_eq!(consumers.get("default"), None);
    }
}


#[test]
fn removing_non_consumer_is_noop() {
    env_logger::init().unwrap_or(());
    let dir = tempdir::TempDir::new("store").expect("store-dir");
    let mut prod = lmqueue::Producer::new(dir.path().to_str().expect("path string")).expect("producer");
    prod.produce(b"0").expect("produce");
    prod.produce(b"1").expect("produce");
    prod.produce(b"2").expect("produce");

    {
        let mut cons = lmqueue::Consumer::new(dir.path().to_str().expect("path string"), "default").expect("consumer");
        let _entry = cons.poll().expect("poll");
    }

    {
        let mut cons = lmqueue::Consumer::new(dir.path().to_str().expect("path string"), "default").expect("consumer");
        cons.clear_offset().expect("clear_offset");
    }
    {
        let cons = lmqueue::Consumer::new(dir.path().to_str().expect("path string"), "default").expect("consumer");
        let consumers = cons.consumers().expect("consumers");
        assert_eq!(consumers.get("default"), None);
    }
}
