extern crate regex;
extern crate redis;

use std::env;
//use std::thread;
use regex::Regex;
use redis::{PubSubCommands, ControlFlow, IntoConnectionInfo};

const APP_NAME: &'static str = env!("CARGO_PKG_NAME");
const APP_VER: &'static str = env!("CARGO_PKG_VERSION");

fn main() {
  println!("{} v{} starting...", APP_NAME, APP_VER);
  let connect_url: String;
  let host_name: String;

  match env::var("REDIS_URL") {
    Ok(r_url) => {
      let filter_re = Regex::new(r"redis://.*?:.*@(.*)(?:/\d+)").expect("Bad redis URL filter regex");
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
      print!("Connecting to redis host '{}'... ", host_name);
      let r_conn = redis::Client::open(c_info).expect("redis::Client::open()");
      let mut conn = r_conn.get_connection().expect("redis::Client::get_connection()");
      println!("success.");
      conn.psubscribe(&["*"], |msg| -> redis::ControlFlow<()> {
        println!("MSG in '{}':\n{}\n", msg.get_channel_name(), msg.get_payload::<String>().unwrap());
        ControlFlow::Continue
      }).expect("psubscribe failed");
    },
    Err(e) => {
      panic!("Bad connection information: {}", e);
    }
  }
}
