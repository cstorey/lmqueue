extern crate lmqueue;
extern crate clap;
#[macro_use]
extern crate log;
extern crate env_logger;
use clap::{Arg, App, SubCommand};

use std::io::{self, Write};
use std::time::{self, Duration};
use std::thread;

use std::process::{Stdio, Command, Child};

const DEFAULT_CONSUMER: &'static str = "default";

fn main() {
    let matches = App::new("listener")
                      .version("???")
                      .author("Ceri Storey")
                      .subcommand(SubCommand::with_name("consume")
                                      .about("pipes each item though a command")
                                      .arg(Arg::with_name("queue").required(true))
                                      .arg(Arg::with_name("name")
                                               .short("n")
                                               .takes_value(true)
                                               .help("consumer name (defaults to `default`)"))
                                      .arg(Arg::with_name("command")
                                               .multiple(true)
                                               .index(2)
                                               .required(true)))
                      .subcommand(SubCommand::with_name("offsets")
                                      .about("list consumer offsets")
                                      .arg(Arg::with_name("queue").required(true)))
                      .get_matches();

    env_logger::init().expect("env_logger::init");

    match matches.subcommand() {
        ("consume", Some(matches)) => {
            process_consumer(matches.value_of("queue").expect("queue"),
                             matches.value_of("name").unwrap_or(DEFAULT_CONSUMER),
                             matches.values_of("command").expect("command").collect())
        }
        ("offsets", Some(matches)) => display_offsets(matches.value_of("queue").expect("queue")),
        other => println!("{}", matches.usage()),
    }
}

fn process_consumer(dir: &str, consumer_name: &str, filter_command: Vec<&str>) {
    let mut consumer = lmqueue::Consumer::new(dir, consumer_name).expect("open");

    let mut command = Command::new(filter_command[0]);
    command.args(&filter_command[1..]);
    command.stdin(Stdio::piped());

    loop {
        while let Some(data) = consumer.poll().expect("poll") {
            trace!("Polled for {:?}", data);
            let mut child = command.spawn().expect("spawn");
            child.stdin.as_mut().expect("stdin").write_all(&data.data).expect("write to child");
            let status = child.wait().expect("child wait");
            debug!("child exited with {:?}", status);
            consumer.commit_upto(&data).expect("commit");
        }
        debug!("sleeping");
        thread::sleep(Duration::from_millis(100));
    }
}

fn display_offsets(dir: &str) {
    let consumer = lmqueue::Consumer::new(dir, DEFAULT_CONSUMER).expect("open");
    for (consumer, offset) in consumer.consumers().expect("consumers") {
        println!("{}\t{}", consumer, offset);
    }
}
