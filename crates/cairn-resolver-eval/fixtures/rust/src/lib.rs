pub mod greeter;
pub mod logger;
pub mod shouter;

pub use greeter::Greeter;
pub use logger::Logger;
pub use shouter::Shouter;

pub fn run() {
    let logger = Logger::new();
    logger.greet("world");
    let shouter = Shouter;
    shouter.greet("rust");
}
