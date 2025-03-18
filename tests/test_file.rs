use std::{
    io::Read,
    path::Path,
    sync::{atomic::AtomicBool, Arc},
};
use usrbio::*;

#[test]
fn test() {
    let test_dir = if let Some(dir) = option_env!("USRBIO_3FS_TEST_DIR") {
        Path::new(dir)
    } else {
        println!("skip test.");
        return;
    };

    let mut file = File::open(test_dir.join("hello.txt")).unwrap();
    let mount_point = file.mount_point().to_owned();
    println!("mount point is {}", mount_point.display());

    let mut content = String::new();
    let len = file.read_to_string(&mut content).unwrap();
    assert_eq!(content.len(), len);

    File::open("/dev/zero").unwrap_err();

    let config = RingConfig::default();
    let ring = Ring::create(&config, &mount_point).unwrap();
    let file = Arc::new(file);

    {
        let len = len;
        let content = content.clone();
        let read_job = ReadJob {
            file: file.clone(),
            offset: 0,
            length: len,
        };
        let job = BatchReadJob {
            jobs: vec![read_job],
            callback: Box::new(move |_, buf, cqes| {
                assert_eq!(cqes[0].result, len as i64);
                assert_eq!(&buf[0..len], content.as_bytes());
            }),
        };
        let mut cqes = [hf3fs_cqe::default()];
        ring.batch_read(job, &mut cqes);
    }

    {
        let len = len;
        let content = content.clone();
        let read_job = ReadJob {
            file: file.clone(),
            offset: 0,
            length: len,
        };
        let finish = Arc::new(AtomicBool::new(false));
        let result_clone = finish.clone();
        let job = BatchReadJob {
            jobs: vec![read_job],
            callback: Box::new(move |_, buf, cqes| {
                assert_eq!(cqes[0].result, len as i64);
                assert_eq!(&buf[0..len], content.as_bytes());
                result_clone.store(true, std::sync::atomic::Ordering::Release);
            }),
        };
        let workers = ReadWorkers::start(&config, 2).unwrap();
        workers.enqueue(job);
        while !finish.load(std::sync::atomic::Ordering::Acquire) {
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    }

    let path = test_dir.join("write.txt");
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open_3fs_file(&path)
        .unwrap();
    let config = RingConfig {
        for_read: false,
        ..Default::default()
    };
    let ring = Ring::create(&config, &mount_point).unwrap();
    let buf = b"hello world!";
    let ret = ring.write(&file, buf, 0).unwrap();
    assert_eq!(ret, buf.len());
    let ret = ring.write(&file, b"\n", buf.len() as _).unwrap();
    assert_eq!(ret, 1);
    drop(file);

    let content = std::fs::read_to_string(&path).unwrap();
    assert_eq!(content, "hello world!\n");
}
