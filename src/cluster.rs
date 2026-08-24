use std::{ net::SocketAddr};
use rand::RngExt;
use tokio::sync::oneshot;

use crate::{controller, error::ConrtollerError};







pub struct Cluster{
    config:Config,
    replica:u64,
}


#[derive(Clone)]
pub struct Config(pub Vec<Controller_Config>);

#[derive(Clone)]
pub struct Controller_Config{
    pub id:u64,
    pub port:u32,
    pub ip:SocketAddr,

    pub election_timer:i32
}



impl Cluster {
    pub fn new() -> Self {
        let replicas=0;
        let mut rng = rand::rng();

        let controllers = vec![
            Controller_Config::new(
                1,
                9093,
                "127.0.0.1:9093".parse().unwrap(),
                rng.random_range(150..=300)
            ),
            Controller_Config::new(
                2,
                9094,
                "127.0.0.1:9094".parse().unwrap(),
                rng.random_range(150..=300)
            ),
            Controller_Config::new(
                3,
                9095,
                "127.0.0.1:9095".parse().unwrap(),
                rng.random_range(150..=300)
            ),
        ];

        Self {
            config: Config(controllers),
            replica:0,
        }
    }

    async fn ExecuteConfig(&self)->Result<(),ConrtollerError>{
        let controllers_config=self.config.clone();
        let mut isleader=false;

        let mut controllers=Vec::new();
        
        for (index, controller) in controllers_config.0.iter().enumerate() {
            
            let mut peer_config=controllers_config.clone().0;
            peer_config.remove(index);

            let controller=controller::Controller::new(controller.clone(),peer_config,isleader).await?;
            controllers.push(controller);

        }

        
        let mut  signals=Vec::new();

        for controller in controllers{
            let (sender,reciver)=oneshot::channel::<u8>();
            signals.push(sender);
            let start_next= controller.Init(reciver);

            start_next.await.unwrap();
        }


        for singal in signals{
            singal.send(1).unwrap();
        }


        Ok(())

    }

}


impl Controller_Config {
    pub fn new(id:u64,port:u32,ip:SocketAddr,timer:i32)->Self{
        Self{
            id,
            port,
            ip,
            election_timer:timer
        }
    }
}



#[derive(Clone)]

struct Partition{
    id:usize,
    file_name:String
}