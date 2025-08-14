use std::{
    path::Path,
    sync::{atomic::AtomicBool, Arc},
};
use usrbio::*;

#[test]
fn test_normal() {
    let test_dir = if let Some(dir) = option_env!("USRBIO_3FS_TEST_DIR") {
        Path::new(dir)
    } else {
        println!("skip test.");
        return;
    };

    // 1. create a file.
    let path = test_dir.join("demo.txt");
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open_3fs_file(&path)
        .unwrap();

    // 2. write content.
    let config = RingConfig::default();
    let mut ring = Ring::create(&config, file.mount_point()).unwrap();
    let buf = b"hello world!";
    let ret = ring.write(&file, buf, 0).unwrap();
    assert_eq!(ret, buf.len());
    let ret = ring.write(&file, b"\n", buf.len() as _).unwrap();
    assert_eq!(ret, 1);
    drop(file);

    // 3. read content.
    let content = std::fs::read_to_string(&path).unwrap();
    assert_eq!(content, "hello world!\n");

    File::open("/dev/zero").unwrap_err();

    let file = File::open(path).unwrap();
    let len = content.len();
    let buf = ring.read(&file, 0, len).unwrap();
    assert_eq!(buf, content.as_bytes());

    let buf = ring.read(&file, 0, 1024).unwrap();
    assert_eq!(buf, content.as_bytes());

    // 4. async read in worker.
    let read_job = SendableReadJob {
        file: Arc::new(file),
        offset: 0,
        length: len,
    };
    let finish = Arc::new(AtomicBool::new(false));
    let result_clone = finish.clone();
    let job = BatchReadJobs {
        jobs: vec![read_job],
        callback: Box::new(move |_, results| {
            assert_eq!(results[0].ret, len as i64);
            assert_eq!(results[0].buf, content.as_bytes());
            result_clone.store(true, std::sync::atomic::Ordering::Release);
        }),
    };
    let workers = ReadWorkers::start(&config, 2).unwrap();
    println!("workers: {:#?}", workers);
    workers.enqueue(job);
    while !finish.load(std::sync::atomic::Ordering::Acquire) {
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
}

#[test]
fn test_batch_write() {
    let test_dir = if let Some(dir) = option_env!("USRBIO_3FS_TEST_DIR") {
        Path::new(dir)
    } else {
        println!("skip test.");
        return;
    };

    const N: usize = 256;
    let mut files = Vec::with_capacity(N);
    let mut bufs = Vec::with_capacity(N);
    for i in 0..N {
        let path = test_dir.join(format!("{i}.txt"));
        files.push(
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open_3fs_file(&path)
                .unwrap(),
        );

        bufs.push(vec![i as u8; i]);
    }

    let mut write_jobs = Vec::with_capacity(N);
    for i in 0..N {
        write_jobs.push((&files[i], bufs[i].as_slice(), 0));
    }

    let config = RingConfig::default();
    let mut ring = Ring::create(&config, files[0].mount_point()).unwrap();
    let results = ring.batch_write(&write_jobs[..N / 10]).unwrap();
    for (idx, &ret) in results.iter().enumerate() {
        assert_eq!(ret, idx as i64);
    }
    let results = ring.batch_write(&write_jobs).unwrap();
    for (idx, &ret) in results.iter().enumerate() {
        assert_eq!(ret, idx as i64);
    }

    let mut read_jobs = Vec::with_capacity(N);
    for i in 0..N {
        read_jobs.push((&files[i], 0, i));
    }
    let results = ring.batch_read(&read_jobs[..N/10]).unwrap();
    for (idx, result) in results.iter().enumerate() {
        assert_eq!(result.ret, idx as i64);
        assert_eq!(result.buf, bufs[idx]);
    }
    let results = ring.batch_read(&read_jobs).unwrap();
    for (idx, result) in results.iter().enumerate() {
        assert_eq!(result.ret, idx as i64);
        assert_eq!(result.buf, bufs[idx]);
    }
}
