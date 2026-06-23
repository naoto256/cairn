pub trait Greeter {
    fn greet(&self, name: &str);
}

pub fn hello(name: &str) -> String {
    format!("hello, {name}")
}
