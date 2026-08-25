use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use tokio::{io::AsyncReadExt, net::{TcpListener, tcp::OwnedWriteHalf}, sync::RwLock};

use crate::server::{self, workers::server_workers};





pub struct Brokers_config{
    id:u64,
    ip:SocketAddr,
    leader_socket_addr:Option<SocketAddr>,

}

impl Brokers_config{
    pub fn new(id: u64, ip: SocketAddr)->Self{
        Self{
            id,
            ip,
            leader_socket_addr: None,
        }
    }


    

    
}


struct broker{
    server:server::init::server,
    id:u64,
    ip:SocketAddr
}


impl broker{
    async fn new(config:&Brokers_config)->Self{
        let listner=CreateBrokerSocket(config.ip.clone()).await;
        let shard_count=3;

        let server=server::init::server::new(shard_count).unwrap();
        Self {  
            server,
            id:config.id,
            ip:config.ip
        }
    }

    async fn StartBroker(self,socket:TcpListener){
        let server=Arc::new(RwLock::new(self.server));
        println!("Server started of Broker of id {}",self.id);

        tokio::spawn(async move{
            let (stream,client_addr,) = match socket.accept().await {
                Ok(connection) =>connection,

                Err(e) => {
                    eprintln!("Failed to accept connection: {}",e);

                    return ;
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
        });
            
    }
}

async fn CreateBrokerSocket(ip:SocketAddr)->TcpListener{
    let sokcet=TcpListener::bind(ip).await.unwrap();
    sokcet
}