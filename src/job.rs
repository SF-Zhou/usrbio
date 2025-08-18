use super::*;

pub trait ReadJob {
    fn file(&self) -> &File;
    fn offset(&self) -> u64;
    fn length(&self) -> usize;
}

impl ReadJob for (&File, u64, usize) {
    fn file(&self) -> &File {
        self.0
    }

    fn offset(&self) -> u64 {
        self.1
    }

    fn length(&self) -> usize {
        self.2
    }
}

impl<F: AsRef<File>> ReadJob for (F, u64, usize) {
    fn file(&self) -> &File {
        self.0.as_ref()
    }

    fn offset(&self) -> u64 {
        self.1
    }

    fn length(&self) -> usize {
        self.2
    }
}

pub struct ReadResult<'a> {
    pub ret: i64,
    pub buf: &'a [u8],
}

pub trait WriteJob {
    fn file(&self) -> &File;
    fn data(&self) -> &[u8];
    fn offset(&self) -> u64;
}

impl WriteJob for (&File, &[u8], u64) {
    fn file(&self) -> &File {
        self.0
    }

    fn data(&self) -> &[u8] {
        self.1
    }

    fn offset(&self) -> u64 {
        self.2
    }
}

impl<F: AsRef<File>> WriteJob for (F, &[u8], u64) {
    fn file(&self) -> &File {
        self.0.as_ref()
    }

    fn data(&self) -> &[u8] {
        self.1
    }

    fn offset(&self) -> u64 {
        self.2
    }
}
