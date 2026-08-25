mod cluster;
mod controller;
pub mod error;

mod brokers;




mod server;
mod consumer;
mod producer;
mod client;

mod producer_benchmark;

mod kafka_producer_benchmark;

#[tokio::main]
async fn main() {
    println!("Hello, world!");

    let a =cluster::Cluster::new();

    match a.ExecuteConfig().await {
        Ok(a)=>{

        }
        Err(e)=>{
            println!("{}",e)
        }
    }

    loop{
    let mut input = String::new();
    std::io::stdin().read_line(&mut input).unwrap();    
    }
}
