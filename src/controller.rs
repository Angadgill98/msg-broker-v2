use std::{collections::HashMap, f32::consts::E, fmt::write, hash::{DefaultHasher, Hash, Hasher}, net::SocketAddr, str::from_utf8, sync::Arc};

use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpSocket, TcpStream, tcp::{OwnedReadHalf, OwnedWriteHalf}}, sync::{RwLock, mpsc, oneshot}};

use crate::{cluster::{self, Controller_Config}, error::ConrtollerError};



use tokio::time::{timeout, Duration};


#[derive(Debug)]

pub struct Controller{
    pub id:u64,
    port:u32,
    ip:SocketAddr,
    socket:TcpListener,
    pub peers_config:Vec<cluster::Controller_Config>,
    pub peer_sockets:RwLock<HashMap<SocketAddr,Arc<peer>>>,


    timer:i32,
    pub leader:RwLock<bool>,
    pub leader_addr:RwLock<Option<SocketAddr>> ,
    term:RwLock<u64>,
    
    clients:RwLock<HashMap<SocketAddr,Arc<RwLock<OwnedWriteHalf>>>> ,
    data:HashMap<String,SocketAddr>,


    broker_sockets:RwLock<HashMap<SocketAddr,TcpStream>>,
    broker_ids:RwLock<HashMap<u64,SocketAddr>>,
    broker_no:u64,


}



#[derive(Debug)]

pub struct peer{
    pub reader:RwLock<OwnedReadHalf>,
    pub writer:RwLock<OwnedWriteHalf>,
    pub id:u64,
    // addr:String
}





impl Controller {
    pub async fn new(controller:Controller_Config,peers_config:Vec<Controller_Config>,isleader:bool,broker_no:u64)->Result<Self,ConrtollerError>{
        let socket=match CreateControllerSocket(controller.ip.clone()).await{
            Ok(a)=>{
                println!("Started teh controller with args-{:?}",controller);
                Ok(a)

            }
            Err(e)=>{
                println!("Failed: controller with args-{:?}",controller);
                Err(e)
            }
        }?;

        // let mut map=HashMap::new();
        // for peer in peers_config.clone(){
        //     if peer.id == controller.id {
        //         continue;
        //     }
        //     let (reader,writer,addr,controller_id)= CreatePeerSockets(peer).await?;
        //     map.insert(addr, peer{
        //         reader,
        //         writer,
        //         id:controller_id,
        //     });
        // }

        Ok(Self{
            id:controller.id,
            port:controller.port,
            ip:controller.ip,
            socket,
            peers_config,
            peer_sockets:RwLock::new(HashMap::new()),
            timer:controller.election_timer,
            leader:RwLock::new(isleader),
            leader_addr:RwLock::new(None) ,
            clients:RwLock::new(HashMap::new()),
            term:RwLock::new(0),
            data:HashMap::new(),
            broker_no,
            broker_sockets:RwLock::new(HashMap::new()),
            broker_ids:RwLock::new(HashMap::new())
        })
    }

    
    
    pub fn Init(mut self,start_loop_signal:oneshot::Receiver<u8>)->oneshot::Receiver<u8>{
        let (server_setup_sender_sign,server_setup_recv_sign)=oneshot::channel::<u8>();
        
        tokio::spawn(async move{
            let controller=Arc::new(self);
            server_setup_sender_sign.send(1).unwrap();

            start_loop_signal.await.unwrap();

            for peer in controller.peers_config.clone(){
                if peer.id == controller.id {
                    continue;
                }
                let (reader,writer,addr,controller_id)= match CreatePeerSockets(peer.clone()).await{
                    Ok(a)=>{
                        // println!("Staeted the peer socket for teh controller with args {:?} \nand teh peer socket config is {:?}",controller,peer);
                        Ok(a)
                    }
                    Err(e)=>{
                        println!("Faile to create peer socket for teh controller with args {:?} \nand teh peer socket config is {:?}",controller,peer);
                        Err(e)
                    }
                }.unwrap();
                
                controller.peer_sockets.write().await.insert(
                    peer.ip,
                    Arc::new(peer {
                        reader:RwLock::new(reader),
                        writer:RwLock::new(writer),
                        id:peer.id,
                    })
                );
            }
   

            let (timer_signal_sender, mut receiver) = mpsc::channel::<u8>(100);


            
            let controller_=Arc::clone(&controller);
            tokio::spawn(async move {
                let controller=controller_;
                loop {
                    tokio::select! {

                        // Heartbeat / reset signal received
                        signal = receiver.recv() => {
                            match signal {
                                Some(_) => {
                                    // println!("Reset election timer");
                                    continue;
                                }

                                None => {
                                    println!("Timer channel closed");
                                    break;
                                }
                            }
                        }

                        // No signal before timeout
                        _ = tokio::time::sleep(Duration::from_millis(controller.timer as u64)) => {

                            println!("Election timeout for {}",controller.id);
                            let mut peers: Vec<(SocketAddr, Arc<peer>)> = {
                                let peer_sockets = controller.peer_sockets.read().await;

                                peer_sockets
                                    .iter()
                                    .map(|(addr, peer)| (*addr, Arc::clone(peer)))
                                    .collect()
                            };
                            
                            let peers_len=peers.len();
                            let mut responses=Vec::new();
                            let mut term_guard = controller.term.write().await;
                            *term_guard += 1;

                            let term=*term_guard;
                            drop(term_guard);
                            for (addr, peer)in peers.iter_mut(){

                                let mut buf=Vec::new();
                                let mut final_buf=Vec::new();

                                let command_buf=b"election";
                                let len=(command_buf.len() as u64).to_be_bytes();

                                final_buf.extend_from_slice(&len);
                                final_buf.extend_from_slice(command_buf);

                                

                                let term_buf=(term).to_be_bytes();
                                let len=(term_buf.len() as u64).to_be_bytes();

                                buf.extend_from_slice(&len);
                                buf.extend_from_slice(&term_buf);


                                let socket_addr_buf = controller.ip.to_string().into_bytes();

                                buf.extend_from_slice(&(socket_addr_buf.len() as u64).to_be_bytes());
                                buf.extend_from_slice(&socket_addr_buf);

                                
                                let len=(buf.len() as u64).to_be_bytes();
                                final_buf.extend_from_slice(&len);
                                final_buf.extend_from_slice(&buf);

                                peer.writer.write().await.write_all(&final_buf).await.unwrap();
                                // println!("sent teh req from contrller {} to peer {}",controller.id,peer.id);
                                let mut len_buf = [0u8; 8];
                                peer.reader.write().await.read_exact(&mut len_buf).await.unwrap();
                                // println!("recive1 the req at contrller {} from peer {}",controller.id,peer.id);

                                let len = u64::from_be_bytes(len_buf) as usize;
                                let mut response=vec![0u8;len];
                                peer.reader.write().await.read_exact(&mut response).await.unwrap();
                                // println!("{:?}",response);
                                // println!("recive2 the req at contrller {} from peer {}",controller.id,peer.id);

                                responses.push(response);


                            }

                            let mut accept_count = 1;
                            let mut reject_count = 0;

                            for response in &responses {
                                match response.as_slice() {
                                    b"accept" => accept_count += 1,
                                    b"reject" => reject_count += 1,
                                    _ => println!("Unknown response: {:?}", response),
                                }
                            }

                            if accept_count>=peers_len/2 {
                                let mut leader=controller.leader.write().await;
                                *leader=true;
                                

                                //this is to when the leader is elcted to inform the other peers 
                                for (addr, peer)in peers.iter_mut(){
                                    let mut buf=Vec::new();
                                    let mut final_buf=Vec::new();

                                    let command_buf=b"leader_elected";
                                    let len=(command_buf.len() as u64).to_be_bytes();

                                    final_buf.extend_from_slice(&len);
                                    final_buf.extend_from_slice(command_buf);

                                    let term_buf=(*controller.term.read().await).to_be_bytes();
                                    let len=(term_buf.len() as u64).to_be_bytes();

                                    buf.extend_from_slice(&len);
                                    buf.extend_from_slice(&term_buf);


                                    let socket_addr_buf = controller.ip.to_string().into_bytes();

                                    buf.extend_from_slice(&(socket_addr_buf.len() as u64).to_be_bytes());
                                    buf.extend_from_slice(&socket_addr_buf);

                                    
                                    let len=(buf.len() as u64).to_be_bytes();
                                    final_buf.extend_from_slice(&len);
                                    final_buf.extend_from_slice(&buf);

                                    peer.writer.write().await.write_all(&final_buf).await.unwrap();
                                }

                                SendHeartBeats(Arc::clone(&controller));
                                break;
                                
                            }
                            
                            
                            
                            // println!("responses are {:?}",responses);
                            continue;
                        }
                    }
                }
            });



            loop{
                let (stream,client_addr)=controller.socket.accept().await.unwrap();

                let (mut reader,writer)=stream.into_split();
            
                let mut clients=controller.clients.write().await;

                clients.insert(client_addr.clone(), Arc::new(RwLock::new(writer)));

                let client_controller=Arc::clone(&controller);

                drop(clients);
                let signal_for_heartbeat=timer_signal_sender.clone();

                tokio::spawn(async move{
                    loop {
                        let mut buf = [0u8; 8];
                        reader.read_exact(&mut buf).await.unwrap();
                            
                        if !*client_controller.leader.read().await {
                            let _ = signal_for_heartbeat.send(1).await;
                        }

                        let len = u64::from_be_bytes(buf) as usize;

                        let mut command_buf = vec![0u8; len];

                        reader.read_exact(&mut command_buf).await.unwrap();




                        let mut payload_len_buf = [0u8; 8];

                        reader.read_exact(&mut payload_len_buf).await.unwrap();

                        let mut payload = vec![0u8; u64::from_be_bytes(payload_len_buf) as usize];

                        reader.read_exact(&mut payload).await.unwrap();


                        client_controller.HandleOperations(command_buf,payload,client_addr).await;

                    }

                });
           
            }

            

        }); 
      
        
        server_setup_recv_sign
        
    }

    fn GetBrokerIDfromHashTopic(&self,topic: &Vec<u8>,brokers_count: usize)->usize{
        let mut hasher =DefaultHasher::new();

        topic.hash(&mut hasher);

        let hash = hasher.finish();

        (hash as usize)% brokers_count
    }
    
    async fn HandleOperations(& self,opeartion_buf:Vec<u8>,payload:Vec<u8>,client_addr:SocketAddr){
        // println!("{:?}",payload);
        // println!("received req on controller {} from addr {} adn teh curertn celitns aree {:?}",self.id,client_addr,self.clients);
        // let(payload,_)=self.Simplify(payload);

        match opeartion_buf.as_slice() {
            b"election"=>{
                let(term_buf,payload)=self.Simplify(payload);

                let (socket_addr_buf,payload)=self.Simplify(payload);

                let term = u64::from_be_bytes(
                    term_buf.as_slice().try_into().unwrap()
                );

                let mut controller_term=self.term.write().await;

                if term <= *controller_term {
                    // Candidate is using an old term.
                    // Reject the election.
                    let response = b"reject";
                    let mut buf = Vec::new();

                    // response length
                    buf.extend_from_slice(&(response.len() as u64).to_be_bytes());

                    // response
                    buf.extend_from_slice(response);

                   let clients = self.clients.read().await;

                    

                    let writer = clients.get(&client_addr).unwrap();

                    writer.write().await.write_all(&buf).await.unwrap();
                    // println!("controller {} voted for the leader {:?}",self.id,buf);

                    // write response
                } 
                else if term > *controller_term {
                    *controller_term= term;


                    let response = b"accept";
                    let mut buf = Vec::new();

                    // response length
                    buf.extend_from_slice(&(response.len() as u64).to_be_bytes());

                    // response
                    buf.extend_from_slice(response);

                    let clients = self.clients.read().await;

                    

                    let writer = clients.get(&client_addr).unwrap();

                    writer.write().await.write_all(&buf).await.unwrap();
                    // println!("controller {} voted for the leader {:?}",self.id,buf);

                } 

            }

            b"leader_elected"=>{
                    let (term_buf, payload) = self.Simplify(payload);
                    let (socket_addr_buf, _) = self.Simplify(payload);

                    let term = u64::from_be_bytes(
                        term_buf.as_slice().try_into().unwrap()
                    );

                    let leader_addr: SocketAddr = String::from_utf8(socket_addr_buf)
                        .unwrap()
                        .parse()
                        .unwrap();

                    // Update term
                    {
                        let mut current_term = self.term.write().await;

                        if term > *current_term {
                            *current_term = term;
                        }
                    }

                    // Save leader address
                    {
                        let mut leader_addr_lock = self.leader_addr.write().await;
                        *leader_addr_lock = Some(leader_addr);
                        
                    }

                    // This controller is not the leader
                    {
                        let mut leader = self.leader.write().await;
                        *leader = false;
                    }

                    println!(
                        "Controller {}: leader elected = {}, term = {}",
                        self.id,
                        leader_addr,
                        term
                    );
            }
           
            b"heartbeat" => {
                // println!("Controller {} received heartbeat", self.id);
            }

            b"create_topic"=>{
                // println!("{:?}",payload);

                let (topic_buf,payload)=self.Simplify(payload);

                let (partition_no_buf,payload)=self.Simplify(payload);

                // let brokerid=self.GetBrokerIDfromHashTopic(&topic_buf,self.peers_config.len());

                let operation_buf=b"topic_insert";

                // let buf=Vec::new();

                let partition_no = u64::from_be_bytes(
                    partition_no_buf
                        .try_into()
                        .expect("partition number must be 8 bytes")
                );

                let mut broker_requests: HashMap<usize, Vec<Vec<u8>>> =HashMap::new();


                // --------------------------------------------------
                // Create request for every partition
                // --------------------------------------------------

            
                let total_partitions = partition_no;
                let broker_count = self.broker_no;

                let base = total_partitions / broker_count;
                let remainder = total_partitions % broker_count;
                
                
                let mut partition_mapping: HashMap<u64, (u64, u64)> = HashMap::new();
                for broker_index in 0..broker_count {

                    let total_partitions = partition_no;
                    let broker_count = self.broker_no;

                    let base = total_partitions / broker_count;
                    let remainder = total_partitions % broker_count;

                    let mut global_partition_id = 0u64;

                    for broker_index in 0..broker_count {

                        let local_partition_count =
                            base + if broker_index < remainder { 1 } else { 0 };

                        if local_partition_count == 0 {
                            continue;
                        }

                        let broker_id = broker_index as u64;

                        // -----------------------------------------
                        // GLOBAL -> BROKER -> LOCAL mapping
                        // -----------------------------------------

                        for local_partition_id in 0..local_partition_count {

                            partition_mapping.insert(
                                global_partition_id,
                                (
                                    broker_id,
                                    local_partition_id as u64,
                                ),
                            );

                            global_partition_id += 1;
                        }

                        // -----------------------------------------
                        // Now construct request for this broker
                        // -----------------------------------------

                        let broker_id_buf = broker_id.to_be_bytes();
                        let broker_id_len = (8u64).to_be_bytes();

                        let req_id = broker_id as i64;
                        let req_id_buf = req_id.to_be_bytes();
                        let req_id_len = (8u64).to_be_bytes();

                        let operation_buf = b"topic_insert";
                        let operation_len =
                            (operation_buf.len() as u64).to_be_bytes();

                        // -----------------------------------------
                        // PAYLOAD
                        //
                        // [topic_len][topic]
                        // [local_partition_count_len][local_partition_count]
                        // [broker_id_len][broker_id]
                        //
                        // This is what broker receives.
                        // -----------------------------------------

                        let topic_len =
                            (topic_buf.len() as u64).to_be_bytes();

                        let partition_count_buf =
                            (local_partition_count as u64).to_be_bytes();

                        let partition_count_len = (8u64).to_be_bytes();

                        let mut payload_buf = Vec::new();

                        payload_buf.extend_from_slice(&topic_len);
                        payload_buf.extend_from_slice(&topic_buf);

                        payload_buf.extend_from_slice(&partition_count_len);
                        payload_buf.extend_from_slice(&partition_count_buf);

                        payload_buf.extend_from_slice(&broker_id_len);
                        payload_buf.extend_from_slice(&broker_id_buf);

                        // -----------------------------------------
                        // REQUEST
                        // -----------------------------------------

                        let payload_len =
                            (payload_buf.len() as u64).to_be_bytes();

                        let mut req = Vec::new();

                        req.extend_from_slice(&req_id_len);
                        req.extend_from_slice(&req_id_buf);

                        req.extend_from_slice(&operation_len);
                        req.extend_from_slice(operation_buf);

                        req.extend_from_slice(&payload_len);
                        req.extend_from_slice(&payload_buf);

                        broker_requests
                            .entry(broker_index as usize)
                            .or_default()
                            .push(req);
                    }
                }
                // --------------------------------------------------
                // Now create BATCH for every broker
                // --------------------------------------------------
                println!("Gloabal mapping for partiotn is   {:?}",partition_mapping);
                for (broker_index, requests) in broker_requests {

                    // Number of requests in THIS broker's batch
                    let request_count =
                        requests.len() as u64;

                    // --------------------------------------------------
                    // Combine all requests
                    // --------------------------------------------------

                    let mut requests_buf = Vec::new();

                    for req in requests {
                        requests_buf.extend_from_slice(&req);
                    }

                    // Length of all requests
                    let batch_length =
                        requests_buf.len() as u64;


                    // --------------------------------------------------
                    // Build BATCH
                    //
                    // [8] request_count
                    // [8] batch_length
                    // [requests]
                    // --------------------------------------------------

                    let mut batch =
                        Vec::with_capacity(
                            16 + requests_buf.len()
                        );

                    // [request_count]
                    batch.extend_from_slice(
                        &request_count.to_be_bytes()
                    );

                    // [batch_length]
                    batch.extend_from_slice(
                        &batch_length.to_be_bytes()
                    );

                    // [requests]
                    batch.extend_from_slice(
                        &requests_buf
                    );


                    // println!(
                    //     "Broker {} batch: {:?}",
                    //     broker_index,
                    //     batch
                    // );

                    let broker_id = broker_index as u64;

                    let broker_id = {
                        let broker_ids = self.broker_ids.read().await;

                        match broker_ids.get(&broker_id) {
                            Some(id) => *id,
                            None => {
                                eprintln!("Broker index {} not found", broker_index);
                                continue;
                            }
                        }
                    };
                    
                    let mut broker_sockets = self.broker_sockets.write().await;

                    let stream = match broker_sockets.get_mut(&broker_id) {
                        Some(stream) => stream,
                        None => {
                            eprintln!(
                                "Socket for broker {} not found",
                                broker_id
                            );
                            continue;
                        }
                    };

                    if let Err(e) = stream.write_all(&batch).await {
                        eprintln!(
                            "Failed to send batch to broker {}: {}",
                            broker_id, e
                        );
                    }


                    // Send `batch` to broker_index here
                }





            }
           
            b"who_leader" => {
                let leader = self.leader_addr.read().await;

                let mut response = Vec::new();

                match *leader {
                    Some(addr) => {
                        let addr_buf = addr.to_string().into_bytes();

                        // address length
                        response.extend_from_slice(
                            &(addr_buf.len() as u64).to_be_bytes()
                        );

                        // address
                        response.extend_from_slice(&addr_buf);
                    }

                    None => {
                        // address length = 0
                        response.extend_from_slice(&(0u64).to_be_bytes());
                    }
                }

                let clients = self.clients.read().await;

                let writer = clients.get(&client_addr).unwrap();

                writer
                    .write()
                    .await
                    .write_all(&response)
                    .await
                    .unwrap();
            }
           
            b"i_am_broker"=>{

                let (broker_ip_buf, payload) = self.Simplify(payload);

                let (broker_id_buf, _payload) = self.Simplify(payload);

                let broker_ip: SocketAddr = String::from_utf8(broker_ip_buf).unwrap().parse().unwrap();
                let broker_id = u64::from_be_bytes(
                    broker_id_buf
                        .try_into()
                        .expect("broker id must be 8 bytes")
                );

                let broker_stream = match TcpStream::connect(broker_ip).await {
                    Ok(stream) => {
                        println!(
                            "Controller {} connected to broker {} at {}",
                            self.id,
                            broker_id,
                            broker_ip
                        );
                        stream
                    }

                    Err(e) => {
                        eprintln!(
                            "Controller {} failed to connect to broker {} at {}: {}",
                            self.id,
                            broker_id,
                            broker_ip,
                            e
                        );
                        return;
                    }
                };

                // broker_id -> broker listening address
                self.broker_ids
                    .write()
                    .await
                    .insert(broker_id, broker_ip);

                // broker_id -> TCP connection to broker's normal listener
                self.broker_sockets
                    .write()
                    .await
                    .insert(broker_ip, broker_stream);

                println!("Broker connected: id={}, ip={}", broker_id, broker_ip);
            }
            
            _=>{

            }
        }
    }

    fn Simplify(&self, payload: Vec<u8>) -> (Vec<u8>, Vec<u8>) {
        let len_buf: [u8; 8] = payload[0..8]
            .try_into()
            .unwrap();

        let len = u64::from_be_bytes(len_buf) as usize;

        let start = 8;
        let end = start + len;

        let data = payload[start..end].to_vec();
        let remaining = payload[end..].to_vec();

        (data, remaining)
    }


}


async fn CreateControllerSocket(ip:SocketAddr)->Result<TcpListener, ConrtollerError>{
    println!("{}",ip);
    let socket=TcpListener::bind(ip).await?;

    Ok(socket)
}


pub async fn CreatePeerSockets(peer_config:Controller_Config)->Result<(OwnedReadHalf,OwnedWriteHalf,String,u64),ConrtollerError>{

    let socket=TcpStream::connect(peer_config.ip.clone()).await?;

    let (read,write)=socket.into_split();

    Ok((read,write,String::new(),peer_config.id))
}




struct LeaderController(Controller);


impl LeaderController{
    fn new(controller:Controller)->Self{
        Self(controller)
    }


    fn SendHearBeatToPeers(&self){

    }
}



fn SendHeartBeats(controller: Arc<Controller>) {
        tokio::spawn(async move {
            println!("Controller {} became LEADER", controller.id);

            loop {
                // Check whether we are still leader
                {
                    let leader = controller.leader.read().await;

                    if !*leader {
                        println!("Controller {} is no longer leader", controller.id);
                        break;
                    }
                }

                // Get the current peers
                let peers: Vec<Arc<peer>> = {
                    let peer_sockets = controller.peer_sockets.read().await;

                    peer_sockets
                        .values()
                        .map(Arc::clone)
                        .collect()
                };

                // Send heartbeat to every peer
                for peer in peers {
                    let command = b"heartbeat";

                    let mut buf = Vec::new();

                    // command length
                    buf.extend_from_slice(&(command.len() as u64).to_be_bytes());

                    // command
                    buf.extend_from_slice(command);

                    // You can add term / leader information here
                    let term = *controller.term.read().await;

                    let term_buf = term.to_be_bytes();

                    buf.extend_from_slice(&(term_buf.len() as u64).to_be_bytes());
                    buf.extend_from_slice(&term_buf);

                    if let Err(e) = peer.writer.write().await.write_all(&buf).await {
                        println!(
                            "Failed to send heartbeat from controller {}: {}",
                            controller.id,
                            e
                        );
                    }
                }

                // Wait before next heartbeat
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        });
    }
