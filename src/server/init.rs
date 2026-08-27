
use std::{
    collections::HashMap,
    error::Error,
    hash::DefaultHasher,
    net::SocketAddr,
    sync::Arc,
};

use std::hash::{Hash, Hasher};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{tcp::OwnedWriteHalf, TcpListener},
    sync::{
        mpsc::{self, Sender},
        
        RwLock,
    },
};

use crate::server::{consumer, topic};
use crate::server::workers::server_workers;
use crate::server::{partition, workers};


#[derive(Debug)]
pub struct server {
    pub shard_map: HashMap<Shard, Arc<RwLock<topic::TopicMap>>>,
    pub clients: HashMap<SocketAddr, Arc<RwLock<OwnedWriteHalf>>>,
    pub shard_count: usize,
    pub request_pool:Sender<(Arc<RwLock<server>>, Vec<u8>, Vec<u8>, SocketAddr,i64)>,
    pub partition_worker_pool: Sender<workers::partition_worker::PartitionPoolRequest>,
    
    pub response_pool: Sender<ResponseRequest>,

    pub consumer_grp:Arc<RwLock<consumer::Consumergrp>>,

    pub leader_socket_addr:Option<SocketAddr>
}

type ResponseRequest = (
    Arc<RwLock<server>>,
    SocketAddr,
    bool,
    Vec<u8>,
    i64
);

#[derive(Hash, Eq, PartialEq)]
#[derive(Debug)]
pub struct Shard(pub usize);

impl server {
    pub fn new(shard_count: usize,leader_addr:Option<SocketAddr>) -> Result<Self, Box<dyn Error + Send + Sync>> {
        if shard_count == 0 {
            return Err("Shard count cannot be zero".into());
        }

        let request_pool =workers::server_workers::RequestPool();

        let partition_worker_pool =workers::partition_worker::PartitionPool(4);

        let response_pool =server::ResponsePool();
        println!("server got leader addr as {:?}",leader_addr);
        Ok(Self {
            shard_map:CreateShardMap(shard_count),
            clients: HashMap::new(),
            shard_count,
            request_pool:request_pool,
            partition_worker_pool,
            response_pool:response_pool,
            consumer_grp:Arc::new(RwLock::new(consumer::Consumergrp{
                grp:HashMap::new(),
                consumers:HashMap::new()
            })),
            leader_socket_addr:leader_addr
            
        })
    }

    
    
    fn ResponsePool()-> mpsc::Sender<ResponseRequest>{
        let (sender, mut queue) =mpsc::channel::<ResponseRequest>(1024);

        tokio::spawn(async move {
            while let Some((
                server,
                client_addr,
                ack,
                response,
                req_id
            )) = queue.recv().await
            {
                let client = {
                    let server_guard =server.read().await;

                    let Some(client) =server_guard.clients.get(&client_addr)
                    else {
                        eprintln!("Client not found: {}",client_addr);
                        continue;
                    };

                    Arc::clone(client)
                };

                let mut client_guard =client.write().await;

                let response_len = response.len() as u64;

                let req_id_buf = req_id.to_be_bytes();
                let req_id_len = req_id_buf.len() as u64;

                let mut output =
                Vec::with_capacity(8 + 8 + 1 + 8 + response.len());

                output.extend_from_slice(&req_id_len.to_be_bytes());
                output.extend_from_slice(&req_id_buf);

                output.push(if ack { 1 } else { 0 });

                output.extend_from_slice(&response_len.to_be_bytes());
                output.extend_from_slice(&response);

                // println!("server is {:?}",server);
                // println!("Server: SEndin reponse {:?}",output);

                if let Err(e) =client_guard.write_all(&output).await{
                    eprintln!("Failed to send response to {}: {}",client_addr,e);
                }
            }
        });

        sender
    }


    pub fn GetShard(&self,topic: &[u8],shard_count: usize) -> Shard {
        let mut hasher =DefaultHasher::new();

        topic.hash(&mut hasher);

        let hash = hasher.finish();

        Shard((hash as usize)% shard_count)
    }

    pub fn GetHash(&self,data: &[u8]) -> u64 {
        let mut hasher =DefaultHasher::new();

        data.hash(&mut hasher);

        hasher.finish()
    }
}

fn CreateShardMap(shard_count: usize,) -> HashMap<Shard,Arc<RwLock<topic::TopicMap>>,> {
    let mut shards =HashMap::new();

    for i in 0..shard_count {
        shards.insert(
            Shard(i),
            Arc::new(RwLock::new(topic::TopicMap::new())),
        );
    }

    shards
}

async fn CreateSocket()-> Result<TcpListener, Box<dyn Error>>{
    let addr =std::env::var("server_addr").map_err(|_| {"Environment variable 'server_addr' not defined"})?;

    let socket =TcpListener::bind(&addr).await?;

    Ok(socket)
}

