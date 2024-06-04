use std::{
    fs::OpenOptions,
    io::{self, Write},
    os::unix::fs::OpenOptionsExt,
};

fn std_write(path: &str, iter: usize, size: usize) -> io::Result<()> {
    let mut f = OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open(path)?;
    for _ in 0..iter {
        let buf = generate_data(size);
        f.write_all(&buf)?;
        f.flush()?;
    }
    f.set_len(0)?;
    Ok(())
}

fn mmap_wirte(path: &str, iter: usize, size: usize) -> io::Result<()> {
    let f = OpenOptions::new()
        .read(true)
        .create(true)
        .append(true)
        .open(path)?;
    f.set_len((iter * size) as u64)?;
    let mut mmap = unsafe { memmap2::MmapMut::map_mut(&f)? };
    for i in 0..iter {
        let buf = generate_data(size);
        mmap[i * size..(i + 1) * size].copy_from_slice(&buf);
        mmap.flush()?;
    }

    f.set_len(0)?;
    Ok(())
}

fn direct_write(path: &str, iter: usize, size: usize) -> io::Result<()> {
    let mut f = OpenOptions::new()
        .create(true)
        .write(true)
        .custom_flags(libc::O_DIRECT)
        .open(path)?;
    for _ in 0..iter {
        let buf = generate_data(size);
        f.write_all(&buf)?;
        f.flush()?;
    }
    f.set_len(0)?;
    Ok(())
}
pub(crate) fn generate_data(size: usize) -> Vec<u8> {
    let mut buf = vec![0u8; size];
    getrandom::getrandom(&mut buf).unwrap();
    buf
}

use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn std_write_benchmark(c: &mut Criterion) {
    let path = "/tmp/testfile";
    let iter = 1000;
    let size = 4 * 4096;
    c.bench_function("std_write", |b| {
        b.iter(|| std_write(black_box(path), black_box(iter), black_box(size)))
    });
}

fn direct_write_benchmark(c: &mut Criterion) {
    let path = "/tmp/testfile";
    let iter = 1000;
    let size = 4 * 4096;
    c.bench_function("direct_write", |b| {
        b.iter(|| direct_write(black_box(path), black_box(iter), black_box(size)))
    });
}
fn mmap_write_benchmark(c: &mut Criterion) {
    let path = "/tmp/testfile";
    let iter = 1000;
    let size = 4 * 4096;
    c.bench_function("mmap_write", |b| {
        b.iter(|| mmap_wirte(black_box(path), black_box(iter), black_box(size)))
    });
}

criterion_group!(
    benches,
    std_write_benchmark,
    direct_write_benchmark,
    mmap_write_benchmark
);
criterion_main!(benches);
