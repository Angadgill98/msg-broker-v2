use std::error::Error;

use tokio::net::{TcpSocket, TcpStream};






async fn CreateSocket() -> Result<TcpStream, Box<dyn Error>> {
    let addr = std::env::var("server_addr")
        .map_err(|_| "Environment variable 'server_addr' not defined")?;

    let stream = TcpStream::connect(addr).await?;

    Ok(stream)
}


pub struct producer{
    topics:Vec<topics>
    
}

impl producer {
    pub fn new()->Self{
        Self{
            topics:Vec::new()
        }
    }

    pub fn insert(&self,final_buf:Vec<u8>){
        
    }

    pub fn insert_data(&self,final_buf:Vec<u8>){

    }


}

struct topics{
    topic_name:Vec<u8>,
    partitions:u64
}