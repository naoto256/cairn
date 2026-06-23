use crate::greeter::{Greeter, hello};

pub struct Logger {
    prefix: String,
}

impl Logger {
    pub fn new() -> Self {
        Self { prefix: "log".into() }
    }
}

impl Greeter for Logger {
    fn greet(&self, name: &str) {
        let msg = hello(name);
        println!("[{}] {}", self.prefix, msg);
    }
}
