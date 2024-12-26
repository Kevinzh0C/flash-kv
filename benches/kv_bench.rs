use criterion::{criterion_group, criterion_main, Bencher, Criterion};
use flash_kv::{
  db::Engine,
  option::Options,
  util::rand_kv::{get_test_key, get_test_value},
};
use rand::Rng;
use std::sync::Arc;
use tempfile::TempDir;

const NUM_PREPOPULATE_ITEMS: usize = 100000;

struct BenchContext {
  engine: Arc<Engine>,
  _temp_dir: TempDir,
}

fn setup_engine() -> BenchContext {
  let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
  let mut option = Options::default();
  option.dir_path = temp_dir.path().to_path_buf();
  let engine = Engine::open(option).expect("Failed to open engine");

  for i in 0..NUM_PREPOPULATE_ITEMS {
    let res = engine.put(get_test_key(i), get_test_value(i));
    assert!(res.is_ok(), "Pre-population put failed for key {}", i);
  }

  BenchContext {
    engine: Arc::new(engine),
    _temp_dir: temp_dir,
  }
}

fn run_bench_with_context<F>(c: &mut Criterion, name: &str, bench_fn: F)
where
  F: Fn(&mut Bencher, &Engine) + 'static,
{
  let context = setup_engine();
  let engine = context.engine.clone();
  c.bench_function(name, move |b| bench_fn(b, &engine));
}

fn bench_put(c: &mut Criterion) {
  let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
  let mut option = Options::default();
  option.dir_path = temp_dir.path().to_path_buf();
  let engine = Arc::new(Engine::open(option).expect("Failed to open engine"));

  let mut rnd = rand::thread_rng();

  c.bench_function("flash-kv-put-bench", move |b| {
    let engine = engine.clone();
    b.iter(|| {
      let i = rnd.gen_range(0..u32::MAX) as usize;
      let _ = engine.put(get_test_key(i), get_test_value(i));
    })
  });
}

fn bench_get_hit(c: &mut Criterion) {
  run_bench_with_context(c, "flash-kv-get-hit-bench", |b, engine| {
    let mut rnd = rand::thread_rng();
    b.iter(|| {
      let i = rnd.gen_range(0..NUM_PREPOPULATE_ITEMS);
      let res = engine.get(get_test_key(i));
      assert!(res.is_ok());
    })
  });
}

fn bench_get_miss(c: &mut Criterion) {
  run_bench_with_context(c, "flash-kv-get-miss-bench", |b, engine| {
    let mut rnd = rand::thread_rng();
    b.iter(|| {
      let i = rnd.gen_range(NUM_PREPOPULATE_ITEMS..(NUM_PREPOPULATE_ITEMS + 100000));
      let res = engine.get(get_test_key(i));
      assert!(res.is_err());
    })
  });
}

fn bench_delete_hit(c: &mut Criterion) {
  run_bench_with_context(c, "flash-kv-delete-hit-bench", |b, engine| {
    let mut rnd = rand::thread_rng();
    use std::cell::Cell;
    thread_local!(static DELETE_INDEX: Cell<usize> = Cell::new(0));

    b.iter(|| {
      let i = DELETE_INDEX.with(|idx| {
        let current = idx.get();
        let next = (current + 1) % NUM_PREPOPULATE_ITEMS;
        idx.set(next);
        current
      });

      let res = engine.delete(get_test_key(i));
      assert!(res.is_ok());
    })
  });
}

fn bench_listkeys(c: &mut Criterion) {
  run_bench_with_context(c, "flash-kv-listkeys-bench", |b, engine| {
    b.iter(|| {
      let res = engine.list_keys();
      assert!(res.is_ok());
    })
  });
}

fn bench_stat(c: &mut Criterion) {
  run_bench_with_context(c, "flash-kv-stat-bench", |b, engine| {
    b.iter(|| {
      let res = engine.get_engine_stat();
      assert!(res.is_ok());
    })
  });
}

criterion_group!(
  benches,
  bench_put,
  bench_get_hit,
  bench_get_miss,
  bench_delete_hit,
  bench_listkeys,
  bench_stat
);
criterion_main!(benches);
