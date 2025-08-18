use std::{
    cell::RefCell,
    collections::{HashMap, VecDeque},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use clap::{command, Parser, ValueEnum};
use human_units::{Duration, Size};
use rand::seq::IndexedRandom;
use tokio::{runtime::Runtime, sync::Mutex, task::JoinHandle};
use usrbio::*;

#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    Bench,
    Check,
    GenFiles,
}

#[derive(Parser, Debug, Clone)]
#[command(version, about = "usrbio tools", long_about = None)]
pub struct Args {
    /// Action.
    #[arg(short, long, default_value = "bench")]
    pub action: Action,

    /// Total number of threads.
    #[arg(long, default_value_t = 32)]
    pub threads: usize,

    /// Block size.
    #[arg(long, value_parser=clap::value_parser!(Size), default_value = "512k")]
    pub bs: Size,

    /// IO depth.
    #[arg(long, default_value_t = 32)]
    pub iodepth: usize,

    /// Duration for bench.
    #[arg(long, value_parser=clap::value_parser!(Duration), default_value = "60s")]
    pub duration: Duration,

    /// Byte content for generate files.
    #[arg(long, default_value_t = 0)]
    pub value: u8,

    /// File count for generate files.
    #[arg(long, default_value_t = 1)]
    pub count: usize,

    /// File length for generate files.
    #[arg(long, value_parser=clap::value_parser!(Size), default_value = "16M")]
    pub length: Size,

    /// Filename prefix for generate files.
    #[arg(long, default_value = "")]
    pub prefix: String,

    /// Whether to print throughput.
    #[arg(long, default_value_t = false)]
    pub print_throughput: bool,

    /// Path.
    #[arg()]
    paths: Vec<PathBuf>,
}

impl Args {
    fn buf_size(&self) -> usize {
        self.bs.0 as usize * self.iodepth
    }

    fn ring_config(&self) -> RingConfig {
        RingConfig {
            entries: self.iodepth,
            buf_size: self.buf_size(),
            ..Default::default()
        }
    }
}

struct State {
    args: Args,
    runtime: Runtime,
    data: Vec<u8>,
}

thread_local! {
    static TLS: RefCell<HashMap<PathBuf, Ring>> = RefCell::new(Default::default());
}

fn tls_ring_with<F, R>(f: F, mount_point: &Path, args: &Args) -> Result<R>
where
    F: FnOnce(&mut Ring) -> Result<R>,
{
    TLS.with(|v| -> Result<R> {
        let mut rings = v.borrow_mut();
        if let Some(ring) = rings.get_mut(mount_point) {
            f(ring)
        } else {
            let mut ring = Ring::create(&args.ring_config(), mount_point)?;
            let result = f(&mut ring);
            rings.insert(mount_point.to_owned(), ring);
            result
        }
    })
}

fn read_file(
    file: &File,
    offset: u64,
    length: u64,
    state: &State,
    bytes: &Arc<AtomicU64>,
) -> Result<(u64, u32)> {
    let bs = state.args.bs.0;
    tls_ring_with(
        |ring| -> Result<(u64, u32)> {
            let count = length.next_multiple_of(bs) / bs;

            let mut read_jobs = Vec::with_capacity(count as usize);
            for i in 0..count {
                let o = offset + i * bs;
                let b = std::cmp::min(bs, length - i * bs);
                read_jobs.push((file, o, b as usize));
            }

            let results = ring.batch_read(&read_jobs)?;
            let mut sum = 0;
            let mut crc = 0;
            for (result, (_file, offset, expected)) in results.into_iter().zip(read_jobs) {
                if result.ret < 0 {
                    return Err(Error::ReadFailed(-result.ret as _));
                }
                let real = result.ret as u64;
                sum += real;
                crc = crc32c::crc32c_append(crc, result.buf);

                if real < expected as u64 {
                    eprintln!(
                        "found a hole at {offset}: expected length {expected} > real length {real}",
                    );
                    return Err(Error::ReadFailed(22));
                }
            }
            bytes.fetch_add(sum, Ordering::AcqRel);
            Ok((sum, crc))
        },
        file.mount_point(),
        &state.args,
    )
}

async fn parallel_read(path: &Path, state: &Arc<State>, bytes: &Arc<AtomicU64>) -> Result<u32> {
    // 1. open file.
    let file = Arc::new(File::open(&path)?);
    let length = file.metadata()?.len();

    // 2. read file.
    let buf_size = state.args.buf_size() as u64;
    let mut begin = 0;
    let mut full_crc = 0;
    while begin < length {
        let count = std::cmp::min(
            state.args.threads,
            ((length - begin).next_multiple_of(buf_size) / buf_size) as usize,
        );

        let mut tasks = Vec::with_capacity(count);
        for idx in 0..count {
            let offset = begin + idx as u64 * buf_size;
            let expected = std::cmp::min(length - offset, buf_size);
            tasks.push(state.runtime.spawn_blocking({
                let file = file.clone();
                let state = state.clone();
                let bytes = bytes.clone();
                move || read_file(&file, offset, expected, &state, &bytes)
            }));
        }

        for task in tasks {
            let result = task.await.unwrap()?;
            let (length, crc) = result;
            begin += length;
            full_crc = crc32c::crc32c_combine(full_crc, crc, length as usize);
        }
    }
    assert_eq!(begin, length);

    Ok(full_crc)
}

fn write_file(
    file: &File,
    data: &[u8],
    offset: u64,
    state: &State,
    bytes: &Arc<AtomicU64>,
) -> Result<(u64, u32)> {
    let length = data.len() as u64;
    let bs = state.args.bs.0;
    tls_ring_with(
        |ring| -> Result<(u64, u32)> {
            let count = length.next_multiple_of(bs) / bs;

            let mut write_job = Vec::with_capacity(count as usize);
            let mut buf = &data[..];
            for i in 0..count {
                let o = offset + i * bs;
                let b = std::cmp::min(bs, length - i * bs);
                write_job.push((file, &buf[..b as usize], o));
                buf = &buf[b as usize..];
            }

            let results = ring.batch_write(&write_job)?;
            let mut sum = 0;
            let mut crc = 0;
            for (ret, (_file, buf, offset)) in results.into_iter().zip(write_job) {
                if ret < 0 {
                    return Err(Error::WriteFailed(-ret as _));
                }
                let real = ret as u64;
                sum += real;
                crc = crc32c::crc32c_append(crc, buf);

                let expected = buf.len() as u64;
                if real < expected {
                    eprintln!(
                        "found a hole at {offset}: expected length {expected} > real length {real}",
                    );
                    return Err(Error::ReadFailed(22));
                }
            }
            bytes.fetch_add(sum, Ordering::AcqRel);
            Ok((sum, crc))
        },
        file.mount_point(),
        &state.args,
    )
}

async fn parallel_write(path: &Path, state: &Arc<State>, bytes: &Arc<AtomicU64>) -> Result<u32> {
    // 1. open file.
    let file = Arc::new(
        std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open_3fs_file(&path)?,
    );
    let length = state.args.length.0;

    // 2. write file.
    let buf_size = state.args.buf_size() as u64;
    let mut begin = 0;
    let mut full_crc = 0;
    while begin < length {
        let count = std::cmp::min(
            state.args.threads,
            ((length - begin).next_multiple_of(buf_size) / buf_size) as usize,
        );

        let mut tasks = Vec::with_capacity(count);
        for idx in 0..count {
            let offset = begin + idx as u64 * buf_size;
            let expected = std::cmp::min(length - offset, buf_size);
            tasks.push(state.runtime.spawn_blocking({
                let file = file.clone();
                let state = state.clone();
                let bytes = bytes.clone();
                move || {
                    write_file(
                        &file,
                        &state.data[..expected as usize],
                        offset,
                        &state,
                        &bytes,
                    )
                }
            }));
        }

        for task in tasks {
            let result = task.await.unwrap()?;
            let (length, crc) = result;
            begin += length;
            full_crc = crc32c::crc32c_combine(full_crc, crc, length as usize);
        }
    }
    assert_eq!(begin, length);

    Ok(full_crc)
}

fn collect_file_paths(inputs: &[PathBuf]) -> Result<Vec<PathBuf>> {
    let mut paths = vec![];
    for path in inputs {
        let meta = path.metadata()?;
        if meta.is_dir() {
            for entry in walkdir::WalkDir::new(&path)
                .into_iter()
                .flat_map(|e| e.ok())
            {
                if entry.file_type().is_file() {
                    paths.push(entry.path().to_owned());
                }
            }
        } else if path.is_file() {
            paths.push(path.clone());
        }
    }
    Ok(paths)
}

fn print_throughput(bytes: &Arc<AtomicU64>) {
    let bytes = bytes.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
        loop {
            interval.tick().await;
            let bytes = bytes.swap(0, Ordering::AcqRel);
            if bytes >= 512 << 20 {
                println!("Throughput: {:.1}GiB/s", bytes as f64 / f64::from(1 << 30));
            } else if bytes >= 512 << 10 {
                println!("Throughput: {:.1}MiB/s", bytes as f64 / f64::from(1 << 20));
            } else if bytes >= 512 {
                println!("Throughput: {:.1}KiB/s", bytes as f64 / f64::from(1 << 10));
            } else {
                println!("Throughput: {}B/s", bytes);
            }
        }
    });
}

async fn bench(state: Arc<State>) -> Result<()> {
    let paths = collect_file_paths(&state.args.paths)?;
    if paths.is_empty() {
        eprintln!("empty path!");
        return Err(Error::InvalidArgument);
    }

    let mut files = Vec::with_capacity(paths.len());
    for path in &paths {
        let file = File::open(path)?;
        let length = file.metadata()?.len();
        files.push((Arc::new(file), length));
    }

    let bytes = Arc::new(AtomicU64::default());
    print_throughput(&bytes);

    let mut tasks = Vec::with_capacity(state.args.threads);
    let start_time = std::time::Instant::now();
    for _ in 0..state.args.threads {
        let state = state.clone();
        let files = files.clone();
        let bytes = bytes.clone();
        tasks.push(tokio::task::spawn_blocking(move || {
            let bs = state.args.bs.0;
            let iodepth = state.args.iodepth;
            let mount_point = files[0].0.mount_point().to_owned();
            while start_time.elapsed() <= state.args.duration.0 {
                let mut jobs = Vec::with_capacity(iodepth);
                for _ in 0..iodepth {
                    let (file, length) = files.choose(&mut rand::rng()).unwrap();
                    let offset = rand::random::<u64>() % (length.next_multiple_of(bs) / bs) * bs;
                    jobs.push((file.clone(), offset, bs as usize));
                }

                let result = tls_ring_with(
                    move |ring| {
                        let results = ring.batch_read(&jobs)?;
                        let mut b = 0;
                        for result in results {
                            if result.ret >= 0 {
                                b += result.ret;
                            }
                        }
                        Ok(b)
                    },
                    &mount_point,
                    &state.args,
                );
                match result {
                    Ok(b) => {
                        bytes.fetch_add(b as u64, Ordering::AcqRel);
                    }
                    Err(e) => eprintln!("read filed: {e}"),
                }
            }
        }));
    }

    for task in tasks {
        task.await.unwrap();
    }

    Ok(())
}

async fn check(state: Arc<State>) -> Result<()> {
    let paths = collect_file_paths(&state.args.paths)?;

    let bytes = Arc::new(AtomicU64::default());
    if state.args.print_throughput {
        print_throughput(&bytes);
    }

    let first_error = Arc::new(Mutex::new(None));
    let threads = state.args.threads;
    let mut tasks: VecDeque<JoinHandle<_>> = VecDeque::with_capacity(threads);
    for path in paths {
        while tasks.len() >= threads {
            if let Some(task) = tasks.pop_front() {
                task.await.unwrap();
            }
        }

        let path = path.clone();
        let state = state.clone();
        let bytes = bytes.clone();
        let first_error = first_error.clone();
        tasks.push_back(tokio::spawn({
            async move {
                let result = parallel_read(&path, &state, &bytes).await;
                match result {
                    Ok(crc) => {
                        println!("{:08X} {}", crc, path.display());
                    }
                    Err(err) => {
                        eprintln!("read {} error: {}", path.display(), err);
                        *first_error.lock().await = Some(err);
                    }
                }
            }
        }));
    }

    for task in tasks {
        task.await.unwrap();
    }

    let first_error = first_error.lock().await.take();
    first_error.map(Err).unwrap_or(Ok(()))
}

async fn gen_files(state: Arc<State>) -> Result<()> {
    let root = state.args.paths.first().unwrap();

    let bytes = Arc::new(AtomicU64::default());
    if state.args.print_throughput {
        print_throughput(&bytes);
    }

    let first_error = Arc::new(Mutex::new(None));
    let threads = state.args.threads;
    let mut tasks: VecDeque<JoinHandle<_>> = VecDeque::with_capacity(threads);
    for idx in 0..state.args.count {
        while tasks.len() >= threads {
            if let Some(task) = tasks.pop_front() {
                task.await.unwrap();
            }
        }

        let path = root.join(format!("{}{idx}", state.args.prefix));
        let state = state.clone();
        let bytes = bytes.clone();
        let first_error = first_error.clone();
        tasks.push_back(tokio::spawn({
            async move {
                let result = parallel_write(&path, &state, &bytes).await;
                match result {
                    Ok(crc) => {
                        println!("{:08X} {}", crc, path.display());
                    }
                    Err(err) => {
                        eprintln!("write {} error: {}", path.display(), err);
                        *first_error.lock().await = Some(err);
                    }
                }
            }
        }));
    }

    for task in tasks {
        task.await.unwrap();
    }

    let first_error = first_error.lock().await.take();
    first_error.map(Err).unwrap_or(Ok(()))
}

fn main() -> Result<()> {
    let args = Args::parse();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(args.threads)
        .max_blocking_threads(args.threads)
        .enable_time()
        .build()?;
    let state = State {
        data: vec![args.value; args.buf_size()],
        args,
        runtime,
    };
    let state = Arc::new(state);

    match &state.args.action {
        Action::Bench => state.runtime.block_on(bench(state.clone())),
        Action::Check => state.runtime.block_on(check(state.clone())),
        Action::GenFiles => state.runtime.block_on(gen_files(state.clone())),
    }
}
