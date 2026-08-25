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
    let controller_config:cluster::Config;
    let (leader_elected_signal,reciver)=tokio::sync::oneshot::channel::<u8>();
    match a.ExecuteConfig().await {
        Ok(a)=>{
            controller_config=a;
        }
        Err(e)=>{
            println!("{}",e);
            return;
        }
    }

    a.ExecuteBrokerConfig(controller_config).await;

    loop{
    let mut input = String::new();
    std::io::stdin().read_line(&mut input).unwrap();    
    }
}
