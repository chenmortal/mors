use std::collections::BTreeMap;
use std::sync::Arc;
// use std::mem::replace;
// use std::sync::Arc;
use std::thread;
// use std::thread;
use std::time::Instant;

use mors_common::test::fill_slice;
use mors_common::ts::KeyTsBorrow;
use mors_skip_list::skip_list::SkipList;
use mors_traits::skip_list::SkipListTrait;

const ITERATIONS: usize = 2;
const ELEMENTS: usize = 1_000_000;
// const ELEMENTS: usize = 100_000;
const KEY_SIZE: usize = 24;
const VALUE_SIZE: usize = 150;
const RNG_SEED: u64 = 3;
/// Returns pairs of key, value
fn gen_pair(rng: &mut fastrand::Rng) -> ([u8; KEY_SIZE], Vec<u8>) {
    let mut key = [0u8; KEY_SIZE];
    fill_slice(&mut key, rng);
    let mut value = vec![0u8; VALUE_SIZE];
    fill_slice(&mut value, rng);

    (key, value)
}

fn make_rng() -> fastrand::Rng {
    fastrand::Rng::with_seed(RNG_SEED)
}

fn make_rng_shards(shards: usize, elements: usize) -> Vec<fastrand::Rng> {
    let mut rngs = vec![];
    let elements_per_shard = elements / shards;
    for i in 0..shards {
        let mut rng = make_rng();
        for _ in 0..(i * elements_per_shard) {
            gen_pair(&mut rng);
        }
        rngs.push(rng);
    }

    rngs
}

fn main() {
    single_skiplist_thread();
    // multi_skiplist_thread();
    // single_btreemap_thread();
    // multi_btreemap_thread();
}
fn multi_skiplist_thread() {
    let size:usize = 4096 * 1024 * 1024;
    let arena = (size as f64 * 1.15) as usize;

    for num_threads in [4, 8, 16, 32] {
        // let skip_lists = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let skip_list = SkipList::new(arena, KeyTsBorrow::cmp).unwrap();

        // let skip_list_arc = Arc::new(parking_lot::RwLock::new(skip_list));
        let skip_list_arc = skip_list.clone();
        let mut rngs = make_rng_shards(num_threads, ELEMENTS);
        let start = Instant::now();

        thread::scope(|s| {
            for _ in 0..num_threads {
                let mut rng = rngs.pop().unwrap();
                let skip_list2 = skip_list_arc.clone();
                // let skip_lists2 = skip_lists.clone();
                s.spawn(move || {
                    for _ in 0..(ELEMENTS / num_threads) {
                        let (key, value) = gen_pair(&mut rng);
                        // {
                        //     let list = skip_list2.read();
                        //     if list.size() >= size {
                        //         drop(list);
                        //         let mut s_list = skip_list2.write();
                        //         let skip_list =
                        //             SkipList::new(arena, KeyTsBorrow::cmp)
                        //                 .unwrap();
                        //         let old = replace(&mut *s_list, skip_list);
                        //         let mut lists = skip_lists2.lock();
                        //         lists.push(old);
                        //     } else {
                        //         drop(list);
                        //     }
                        // }
                        // let list = skip_list2.write();
                        // assert!(list.push(&key, value.as_ref()).is_ok());
                        // assert!(skip_list2
                        //     .push(key.as_ref(), value.as_ref())
                        //     .is_ok());
                        match skip_list2.push(key.as_ref(), value.as_ref()) {
                            Ok(_) => {}
                            Err(e) => {
                                eprintln!("Error: {:?}", e);
                            }
                        };
                    }
                });
            }
        });

        let end = Instant::now();
        let duration = end - start;
        println!(
            "write read ({} threads) {} items in {}ms",
            num_threads,
            ELEMENTS,
            duration.as_millis()
        );

        let mut rngs = make_rng_shards(num_threads, ELEMENTS);
        let start = Instant::now();
        thread::scope(|s| {
            for _ in 0..num_threads {
                let mut rng = rngs.pop().unwrap();
                let skip_list2 = skip_list_arc.clone();
                s.spawn(move || {
                    for _ in 0..(ELEMENTS / num_threads) {
                        let (key, value) = gen_pair(&mut rng);
                        let v = skip_list2.get(key.as_ref()).unwrap().unwrap();
                        assert_eq!(value.as_slice(), v);
                    }
                });
            }
        });
        let dur = start.elapsed();
        println!(
            "read ({} threads) {} items in {}ms",
            num_threads,
            ELEMENTS,
            dur.as_millis()
        );
        println!();
    }
}
// fn(&[u8], &[u8]) -> std::cmp::Ordering
fn cmp(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
    a.cmp(b)
}
fn single_skiplist_thread() {
    let size = 256 * 1024 * 1024;
    let arena = (size as f64 * 1.15) as usize;
    let mut skip_lists = Vec::new();
    let mut skip_list = SkipList::new(arena, cmp).unwrap();
    let mut rng = make_rng();
    let start_w = Instant::now();
    for _i in 0..ELEMENTS {
        if skip_list.size() >= size {
            skip_lists.push(skip_list);
            skip_list = SkipList::new(arena, cmp).unwrap();
        };
        let (key, value) = gen_pair(&mut rng);
        assert!(skip_list.push(key.as_ref(), value.as_ref()).is_ok());
    }
    let elapsed_w = start_w.elapsed();
    println!("write elapsed: {:?}", elapsed_w);
    println!(
        "write ops/sec: {}",
        ELEMENTS as f64 / elapsed_w.as_secs_f64()
    );
    for list in &skip_lists {
        print!("{} ", list.size());
    }
    println!();

    for _ in 0..ITERATIONS {
        let mut rng = make_rng();
        let start = Instant::now();
        // let mut checksum = 0u64;
        // let mut expected_checksum = 0u64;
        for _ in 0..ELEMENTS {
            let (key, value) = gen_pair(&mut rng);
            for list in &skip_lists {
                if let Some(fv) = list.get(&key).unwrap() {
                    assert_eq!(value.as_slice(), fv);
                    continue;
                };
            }
        }
        let end = Instant::now();
        let duration = end - start;
        println!(
            "Random read {} items in {}ms",
            ELEMENTS,
            duration.as_millis()
        );
    }
}
fn single_btreemap_thread() {
    let mut tree = BTreeMap::new();
    let mut rng = make_rng();
    let start_w = Instant::now();
    for _i in 0..ELEMENTS {
        let (key, value) = gen_pair(&mut rng);
        tree.insert(key, value);
    }
    let elapsed_w = start_w.elapsed();
    println!("write elapsed: {:?}", elapsed_w);
    println!(
        "write ops/sec: {}",
        ELEMENTS as f64 / elapsed_w.as_secs_f64()
    );
    println!("{}", tree.len());
    println!();
    for _ in 0..ITERATIONS {
        let mut rng = make_rng();
        let start = Instant::now();
        // let mut checksum = 0u64;
        // let mut expected_checksum = 0u64;
        for _ in 0..ELEMENTS {
            let (key, value) = gen_pair(&mut rng);
            if let Some(fv) = tree.get(&key) {
                assert_eq!(value.as_slice(), fv);
            }
        }
        let end = Instant::now();
        let duration = end - start;
        println!(
            "Random read {} items in {}ms",
            ELEMENTS,
            duration.as_millis()
        );
    }
}
fn multi_btreemap_thread() {
    for num_threads in [4, 8, 16, 32, 64] {
        // let skip_lists = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let mut tree = Arc::new(parking_lot::RwLock::new(BTreeMap::new()));
        let mut rngs = make_rng_shards(num_threads, ELEMENTS);
        let start = Instant::now();

        thread::scope(|s| {
            for _ in 0..num_threads {
                let mut rng = rngs.pop().unwrap();
                let tree2 = tree.clone();
                s.spawn(move || {
                    for _ in 0..(ELEMENTS / num_threads) {
                        let (key, value) = gen_pair(&mut rng);
                        tree2.write().insert(key, value);
                    }
                });
            }
        });

        let end = Instant::now();
        let duration = end - start;
        println!(
            "write read ({} threads) {} items in {}ms",
            num_threads,
            ELEMENTS,
            duration.as_millis()
        );

        let mut rngs = make_rng_shards(num_threads, ELEMENTS);
        let start = Instant::now();
        thread::scope(|s| {
            for _ in 0..num_threads {
                let mut rng = rngs.pop().unwrap();
                // let skip_list2 = skip_list_arc.clone();
                let tree2 = tree.clone();
                s.spawn(move || {
                    for _ in 0..(ELEMENTS / num_threads) {
                        let (key, value) = gen_pair(&mut rng);

                        assert_eq!(
                            tree2.read().get(&key).unwrap().as_slice(),
                            value
                        );
                    }
                });
            }
        });
        let dur = start.elapsed();
        println!(
            "read ({} threads) {} items in {}ms",
            num_threads,
            ELEMENTS,
            dur.as_millis()
        );
        println!();
    }
}
