extern crate config;
extern crate redis;

use std::env;
use std::thread;
use std::collections::HashMap;
use config::{Config, File, Environment};
use redis::{PubSubCommands, ControlFlow};

const APP_NAME: &'static str = env!("CARGO_PKG_NAME");
const APP_VER: &'static str = env!("CARGO_PKG_VERSION");

fn monitor_all(r_client: &redis::Client) -> thread::JoinHandle<()> {
  print!("monitor_all connecting...");
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
      println!("({}) {}: {}", total_count, &chan_name, *chan_entry);
      ControlFlow::Continue
    };

    conn.psubscribe(&["*"], msg_handler).expect("psubscribe");
  })
}

fn watch_devices(r_client: &redis::Client, watch: Vec<String>) -> thread::JoinHandle<()> {
  print!("watch_devices connecting...");
  let mut conn = r_client.get_connection().expect("get_connection()");
  println!("success.");

  thread::spawn(move || {
    if watch.len() > 0 {
      println!("NEED TO WATCH <{:?}>", watch);
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
      settings.get::<u16>("redis.port").unwrap_or(6379))
    ),
    db: 0, 
    passwd: match settings.get_str("redis.password") {
      Ok(pass) => Some(pass),
      Err(_) => None
    }
  };
  
  let r_conn = redis::Client::open(r_conn_info).expect("redis::Client::open()");

  for jh in vec![
    monitor_all(&r_conn), 
    watch_devices(&r_conn, settings.get::<Vec<String>>("redis.watch").unwrap_or(vec![]))
  ] {
    jh.join().expect("join");
  }
}
