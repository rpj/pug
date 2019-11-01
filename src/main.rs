extern crate regex;
extern crate redis;

use std::env;
use std::collections::HashMap;
use regex::Regex;
use redis::{PubSubCommands, ControlFlow, IntoConnectionInfo};

const APP_NAME: &'static str = env!("CARGO_PKG_NAME");
const APP_VER: &'static str = env!("CARGO_PKG_VERSION");

fn monitor_pubsub_channels(host_name: &String, r_client: &redis::Client) -> () {
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
  
  print!("Connecting to redis host '{}'...", host_name);
  let mut conn = r_client.get_connection().expect("redis::Client::get_connection()");
  println!("success.");
  match conn.psubscribe(&["*"], msg_handler) {
    Ok(_) => (),
    Err(e) => panic!("{}", e)
  }
}

fn main() {
  println!("{} v{} starting...", APP_NAME, APP_VER);
  let connect_url: String;
  let host_name: String;

  match env::var("REDIS_URL") {
    Ok(r_url) => {
      let filter_re = Regex::new(r"redis://(?:(?:.*?:)?.*@)?([^/]+)(?:/\d+)?").expect("Bad redis URL filter regex");
      let caps = filter_re.captures(&r_url).expect("captures() failed");
      if caps.len() < 2 {
        panic!("Badly-formatted redis URL '{}'", r_url);
      }
      connect_url = r_url.clone();
      host_name = caps[1].to_string();
    },
    Err(_e) => {
      panic!("'REDIS_URL' is not defined in the environment");
    }
  }

  match connect_url.into_connection_info() {
    Ok(c_info) => {
      let r_conn = redis::Client::open(c_info).expect("redis::Client::open()");
      monitor_pubsub_channels(&host_name, &r_conn);
    },
    Err(e) => {
      panic!("Bad connection information: {}", e);
    }
  }
}
