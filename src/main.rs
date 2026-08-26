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

    let brokers_config=a.ExecuteBrokerConfig(controller_config).await;

    // // Now server is ready
    // let mut client = match client::init::client::init(brokers_config).await {
    //     Ok(client) => client,
    //     Err(e) => {
    //         eprintln!("Failed to create client: {}", e);
    //         return;
    //     }
    // };

    



    let mut client=client::init::client::new(brokers_config).await;
    if let Err(e) =client::cli::Cli::init(&mut client).await{
        eprintln!("CLI error: {}", e);
    }

    // client.get_leader_stream().await.unwrap();

    loop{
    let mut input = String::new();
    std::io::stdin().read_line(&mut input).unwrap();    
    }
}
