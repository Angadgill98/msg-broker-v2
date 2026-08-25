
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
    pub request_pool:Sender<(Arc<RwLock<server>>, Vec<u8>, Vec<u8>, SocketAddr)>,
    pub partition_worker_pool: Sender<workers::partition_worker::PartitionPoolRequest>,
    
    pub response_pool: Sender<ResponseRequest>,

    pub consumer_grp:Arc<RwLock<consumer::Consumergrp>>
}

type ResponseRequest = (
    Arc<RwLock<server>>,
    SocketAddr,
    bool,
    Vec<u8>,
);

#[derive(Hash, Eq, PartialEq)]
#[derive(Debug)]
pub struct Shard(pub usize);

impl server {
    pub fn new(shard_count: usize) -> Result<Self, Box<dyn Error + Send + Sync>> {
        if shard_count == 0 {
            return Err("Shard count cannot be zero".into());
        }

        let request_pool =workers::server_workers::RequestPool();

        let partition_worker_pool =workers::partition_worker::PartitionPool(4);

        let response_pool =server::ResponsePool();

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
            }))
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

                let response_len =response.len() as u64;

                let mut output =Vec::with_capacity(1 + 8 + response.len());

                output.push(if ack { 1 } else { 0 });

                output.extend_from_slice(&response_len.to_be_bytes());

                output.extend_from_slice(&response);

                // println!("server is {:?}",server);

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

pub async fn Init(server_ready:tokio::sync::oneshot::Sender<()>,) {
    let socket =
        match CreateSocket().await {
            Ok(socket) => socket,

            Err(e) => {
                eprintln!("Failed to create server socket: {}",e);

                return;
            }
        };

    let shard_count: usize = 10;

    let server =match server::new(shard_count) {
            Ok(server) => {
                Arc::new(RwLock::new(server))
            }

            Err(e) => {
                eprintln!("Failed to initialize server: {}",e);
                return;
            }
        };

    if server_ready.send(()).is_err() {
        eprintln!("Failed to signal server readiness");
    }

    println!("Server started");

    loop {
        let (stream,client_addr,) = match socket.accept().await {
            Ok(connection) =>connection,

            Err(e) => {
                eprintln!("Failed to accept connection: {}",e);

                continue;
            }
        };
        println!("Client connected: {}",client_addr);

        let server =Arc::clone(&server);

        tokio::spawn(async move {
            let (mut reader,writer,) = stream.into_split();

            {
                let mut server_guard =server.write().await;
                let writer_stream =Arc::new(RwLock::new(writer));
                server_guard.clients.insert(client_addr,writer_stream,);
            }

            let server =Arc::clone(&server);
            let addr=client_addr.to_string();

            'connection: loop {
                let mut count_buf =[0u8; 8];

                if let Err(e) =reader.read_exact(&mut count_buf).await{
                    eprintln!("Client {} disconnected: {}",client_addr,e);
                    break 'connection;
                }

                let request_count =u64::from_be_bytes(count_buf) as usize;

                if request_count == 0 {
                    eprintln!("Client {} sent empty batch",client_addr);
                    continue;
                }

                let mut allreq_buf_len =[0u8; 8];

                if let Err(e) =reader.read_exact(&mut allreq_buf_len).await{
                    eprintln!("Failed reading batch length from {}: {}",client_addr,e);
                    break 'connection;
                }

                let len =u64::from_be_bytes(allreq_buf_len) as usize;

                let mut all_req_buf =vec![0u8; len];

                if let Err(e) =reader.read_exact(&mut all_req_buf).await{
                    eprintln!("Failed reading batch data from {}: {}",client_addr,e);
                    break 'connection;
                }

                let mut remaining_buf =all_req_buf;

                for request_index in 0..request_count{
                    let (request,remaining) = match server_workers::Simplify(remaining_buf) {
                        Ok(result) =>
                            result,

                        Err(e) => {
                            eprintln!("Failed to parse request {} from {}: {}",request_index,client_addr,e);
                            break 'connection;
                        }
                    };

                    remaining_buf =remaining;

                    let (operation,mut payload,) = match server_workers::Simplify(request) {
                        Ok(result) =>result,

                        Err(e) => {
                            eprintln!("Failed to parse operation/payload for request {} from {}: {}",request_index,client_addr,e);
                            break 'connection;
                        }
                    };

                    let request_pool = {
                        let server_guard =server.read().await;

                        server_guard.request_pool.clone()
                    };

                    // let addr_buf=addr.as_bytes();
                    // let addr_len=(addr_buf.len() as u64).to_be_bytes();

                    // payload.extend_from_slice(&addr_len);
                    // payload.extend_from_slice(addr_buf);

                    if let Err(e) =request_pool
                        .send((
                            Arc::clone(
                                &server
                            ),
                            operation,
                            payload,
                            client_addr,
                        ))
                        .await
                    {
                        eprintln!("Failed to queue request from {}: {}",client_addr,e);

                        break 'connection;
                    }
                }

                if !remaining_buf.is_empty() {
                    eprintln!("Batch from {} contained {} unconsumed bytes",client_addr,remaining_buf.len());
                }
            }

            {
                let mut server_guard =server.write().await;

                server_guard.clients.remove(&client_addr);
            }

            println!("Client {} connection handler stopped",client_addr);
        });
    }
}


async fn CreateSocket()-> Result<TcpListener, Box<dyn Error>>{
    let addr =std::env::var("server_addr").map_err(|_| {"Environment variable 'server_addr' not defined"})?;

    let socket =TcpListener::bind(&addr).await?;

    Ok(socket)
}

