use crate::greeter::Greeter;

pub struct Shouter;

impl Greeter for Shouter {
    fn greet(&self, name: &str) {
        println!("HELLO, {}!", name.to_uppercase());
    }
}
