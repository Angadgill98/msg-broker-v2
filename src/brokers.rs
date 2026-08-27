use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream, tcp::{OwnedReadHalf, OwnedWriteHalf}}, sync::RwLock};

use crate::{cluster::{self, Config}, server::{self, workers::server_workers}};




#[derive(Clone)]
pub struct Brokers_config{
    id:u64,
    pub ip:SocketAddr,
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


pub struct broker{
    server:server::init::server,
    id:u64,
    pub ip:SocketAddr,
    pub controllers:HashMap<SocketAddr,TcpStream>,
    leader_socket_addr:Option<SocketAddr>,
}


impl broker{
    pub async fn new(config:&Brokers_config,controller_config:Config)->Self{
        let listner=CreateBrokerSocket(config.ip.clone()).await;
        let shard_count=3;
        let map=broker::CreatePeerConnection(controller_config).await;
        let server=server::init::server::new(shard_count,config.leader_socket_addr.clone()).unwrap();
        Self {  
            server,
            id:config.id,
            ip:config.ip,
            controllers:map,
            leader_socket_addr:config.leader_socket_addr
        }
    }

    pub async fn StartBroker(mut self,socket:TcpListener){
        

        loop {
            let mut found_leader = false;

            for (_socket_addr, stream) in self.controllers.iter_mut() {
                let command = b"who_leader";
                let len = (command.len() as u64).to_be_bytes();

                let mut buf = Vec::new();

                buf.extend_from_slice(&len);
                buf.extend_from_slice(command);

                // payload length = 0
                buf.extend_from_slice(&[0u8; 8]);

                stream.write_all(&buf).await.unwrap();

                let mut len_buf = [0u8; 8];

                if let Err(e) = stream.read_exact(&mut len_buf).await {
                    eprintln!("Failed to read leader response: {}", e);
                    continue;
                }

                let addr_len = u64::from_be_bytes(len_buf) as usize;

                // This controller doesn't know a leader yet
                if addr_len == 0 {
                    continue;
                }

                let mut socket_addr_buf = vec![0u8; addr_len];

                if let Err(e) = stream.read_exact(&mut socket_addr_buf).await {
                    eprintln!("Failed to read leader address: {}", e);
                    continue;
                }

                let socket_addr = match String::from_utf8(socket_addr_buf)
                    .ok()
                    .and_then(|s| s.parse::<SocketAddr>().ok())
                {
                    Some(addr) => addr,
                    None => continue,
                };

                self.leader_socket_addr = Some(socket_addr);

                println!(
                    "Broker {} found leader at {}",
                    self.id,
                    socket_addr
                );

                found_leader = true;

                self.server.leader_socket_addr=Some(socket_addr);
                break;
            }

            if found_leader {
                break;
            }

            // No controller knows the leader yet.
            println!(
                "Broker {}: no leader yet, retrying...",
                self.id
            );

            tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
        }

       

       let leader_controller_stream={
            let leader_addr=self.leader_socket_addr.unwrap();
            let stream=self.controllers.get_mut(&leader_addr).unwrap();
            stream
       };

       {
        let mut buf=Vec::new();
        let mut final_buf=Vec::new();

        let command_buf=b"i_am_broker";
        let command_len=(command_buf.len() as u64).to_be_bytes();

        final_buf.extend_from_slice(&command_len);
        final_buf.extend_from_slice(command_buf);



        let broker_socket_ip=self.ip.to_string().as_bytes().to_vec();
        let len=(broker_socket_ip.len() as u64).to_be_bytes();

        buf.extend_from_slice(&len);
        buf.extend_from_slice(&broker_socket_ip);


        let broker_id=self.id.to_be_bytes();
        let id_len=(broker_id.len() as u64).to_be_bytes();

        buf.extend_from_slice(&id_len);
        buf.extend_from_slice(&broker_id);


        let len=(buf.len() as u64).to_be_bytes();
        final_buf.extend_from_slice(&len);
        final_buf.extend_from_slice(&buf);



        leader_controller_stream.write_all(&final_buf).await.unwrap();

       }




        tokio::spawn(async move{
            
            let broker=self;
            let server=Arc::new(RwLock::new(broker.server));
            println!("Server started of Broker of id {}",broker.id);
            loop {
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

                        // println!("Server:All req buf is {:?}",remaining_buf);

                        for request_index in 0..request_count{
                            // -----------------------------------------
                            // Parse request ID
                            // -----------------------------------------

                            let (req_id_buf, remaining) =
                                match server_workers::Simplify(remaining_buf) {
                                    Ok(result) => result,

                                    Err(e) => {
                                        eprintln!(
                                            "Failed to parse request ID {} from {}: {}",
                                            request_index,
                                            client_addr,
                                            e
                                        );
                                        break 'connection;
                                    }
                                };

                            remaining_buf = remaining;

                            let req_id = match req_id_buf.len() {
                                8 => {
                                    let mut id_buf = [0u8; 8];
                                    id_buf.copy_from_slice(&req_id_buf);

                                    i64::from_be_bytes(id_buf)
                                }

                                _ => {
                                    eprintln!(
                                        "Invalid request ID length {} from {}",
                                        req_id_buf.len(),
                                        client_addr
                                    );
                                    break 'connection;
                                }
                            };

                            let (operation,remaining) = match server_workers::Simplify(remaining_buf) {
                                Ok(result) =>
                                    result,

                                Err(e) => {
                                    eprintln!("Failed to parse request {} from {}: {}",request_index,client_addr,e);
                                    break 'connection;
                                }
                            };

                            // println!("Server:operation buf is {:?}",operation);
                            let (payload,remaining,) = match server_workers::Simplify(remaining) {
                                Ok(result) =>result,

                                Err(e) => {
                                    eprintln!("Failed to parse operation/payload for request {} from {}: {}",request_index,client_addr,e);
                                    break 'connection;
                                }
                            };
                            remaining_buf =remaining.clone();


                            let request_pool = {
                                let server_guard =server.read().await;

                                server_guard.request_pool.clone()
                            };

                            // let addr_buf=addr.as_bytes();
                            // let addr_len=(addr_buf.len() as u64).to_be_bytes();

                            // payload.extend_from_slice(&addr_len);
                            // payload.extend_from_slice(addr_buf);
                            // println!("final ahdnle op op and payload {:?}  {:?}",operation,payload);
                            if let Err(e) =request_pool
                                .send((
                                    Arc::clone(
                                        &server
                                    ),
                                    operation,
                                    payload,
                                    client_addr,
                                    req_id
                                ))
                                .await
                            {
                                eprintln!("Failed to queue request from {}: {}",client_addr,e);

                                break 'connection;
                            }
                        }

                        // if !remaining_buf.is_empty() {
                        //     eprintln!("Batch from {} contained {} unconsumed bytes",client_addr,remaining_buf.len());
                        // }
                    }

                    {
                        let mut server_guard =server.write().await;

                        server_guard.clients.remove(&client_addr);
                    }

                    println!("Client {} connection handler stopped",client_addr);
                });
            }
            
        });
            
    }

    pub async fn CreatePeerConnection(configs:cluster::Config)->HashMap<SocketAddr,TcpStream>{
        let mut map=HashMap::new();

        for config in configs.0.iter(){
            let socket=TcpStream::connect(config.ip.clone()).await.unwrap();
            map.insert(config.ip, socket);
        }

        return map;
    }

    fn ConnectToLeader(addr:SocketAddr){

    }

  
}

pub async fn CreateBrokerSocket(ip:SocketAddr)->TcpListener{
    let sokcet=TcpListener::bind(ip).await.unwrap();
    sokcet
}

