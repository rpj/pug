extern crate config;
extern crate redis;

use std::{env, thread, time};
use std::collections::HashMap;
use config::{Config, File, Environment};
use redis::{Commands, PubSubCommands, ControlFlow};

const APP_NAME: &'static str = env!("CARGO_PKG_NAME");
const APP_VER: &'static str = env!("CARGO_PKG_VERSION");

const DEFAULT_REDIS_PORT: u16 = 6379;
const DEFAULT_WAIT_FUDGE_PERCENT: f64 = 0.95;

fn monitor_all(r_client: &redis::Client, verbose: bool) -> thread::JoinHandle<()> {
  print!("monitor_all verbose={}; connecting...", verbose);
  let mut conn = r_client.get_connection().expect("get_connection()");
  println!("success.");

  thread::spawn(move || {
    let mut total_count: i64 = 0;
    let mut channel_tracker: HashMap<String, i64> = HashMap::new();

    let msg_handler = |msg: redis::Msg| -> redis::ControlFlow<()> {
      let chan_name = msg.get_channel_name().to_string();
      let chan_entry = channel_tracker.entry(chan_name.clone()).or_insert(0);
      total_count += 1;
      *chan_entry += 1;
      if verbose { println!("monitor_all> ({}) {}: {}", total_count, &chan_name, *chan_entry); }
      else if total_count % 100 == 0 { println!("monitor_all> {} messages seen", total_count); }
      ControlFlow::Continue
    };

    conn.psubscribe(&["*"], msg_handler).expect("psubscribe");
  })
}

fn watch_devices(r_client: &redis::Client, watch: Vec<String>, fudge: f64) -> thread::JoinHandle<()> {
  print!("watch_devices has watch.len={}, fudge={}; connecting...", watch.len(), fudge);
  let mut conn = r_client.get_connection().expect("get_connection()");
  println!("success.");

  thread::spawn(move || {
    let mut all_keys: HashMap<String, i64> = HashMap::new();
    let mut min_ttl = std::i64::MAX;
    let mut prev_count = 0;

    for wp in watch {
      let watch_keys = conn.keys::<String, Vec<String>>(wp.clone()).unwrap_or(vec![]);
      println!("pattern '{}' has {} to watch", wp, watch_keys.len());
      for wk in watch_keys {
        let ttl = conn.ttl::<String, i64>(wk.clone()).expect("ttl");
        println!("'{}' has ttl {}", wk, ttl);
        all_keys.entry(wk).or_insert(ttl);
        min_ttl = if ttl < min_ttl { ttl } else { min_ttl };
        prev_count += 1;
      }
    }

    loop {
      let sleep_ms: u64 = ((min_ttl as f64 * fudge) * 1000.0) as u64;
      println!("watch thread sleeping for {}ms", sleep_ms);
      thread::sleep(time::Duration::from_millis(sleep_ms));
      let mut count = 0;
      let new_ttls = all_keys.keys().map(|k| {
          count += 1;
          let ttl = conn.ttl::<String, i64>(k.clone()).expect("ttl");
          //all_keys.insert(k.clone(), ttl);
          println!("{} ttl -> {}", k, ttl);
          ttl
        });
      min_ttl = new_ttls.min().expect("new min ttl");
      println!("WATCH THREAD AWAKE new min {}, {} vs {}", min_ttl, prev_count, count);
      if prev_count < count {
        println!("!!!!! Lost one!");
      }
      prev_count = count;
    }
  })
}

fn main() {
  println!("{} v{} starting...", APP_NAME, APP_VER);

  let mut settings = Config::default();
  settings
    .merge(File::with_name("config/default").required(false)).unwrap()
    .merge(File::with_name("config/local").required(false)).unwrap()
    .merge(Environment::default().separator("_").prefix(APP_NAME)).unwrap();

  let r_host = settings.get_str("redis.host").expect("redis.host");
  println!("using redis host {}", r_host);

  let r_conn_info = redis::ConnectionInfo {
    addr: Box::new(redis::ConnectionAddr::Tcp(
      r_host, 
      settings.get::<u16>("redis.port").unwrap_or(DEFAULT_REDIS_PORT))
    ),
    db: 0, 
    passwd: match settings.get_str("redis.password") {
      Ok(pass) => Some(pass),
      Err(_) => None
    }
  };
  
  let r_conn = redis::Client::open(r_conn_info).expect("redis::Client::open()");

  for jh in vec![
    monitor_all(&r_conn, settings.get_bool("monitor.verbose").unwrap_or(false)), 
    watch_devices(&r_conn, 
      settings.get::<Vec<String>>("redis.watch.patterns").unwrap_or(vec![]),
      settings.get_float("redis.watch.ttl_percentage").unwrap_or(DEFAULT_WAIT_FUDGE_PERCENT)
    )
  ] {
    jh.join().expect("join");
  }
}
