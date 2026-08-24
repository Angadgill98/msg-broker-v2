use std::{collections::HashMap, fmt::write, net::SocketAddr, str::from_utf8, sync::Arc};

use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpSocket, TcpStream, tcp::{OwnedReadHalf, OwnedWriteHalf}}, sync::{RwLock, oneshot}};

use crate::{cluster::{self, Controller_Config}, error::ConrtollerError};



use tokio::time::{timeout, Duration};

pub struct Controller{
    id:u64,
    port:u32,
    ip:SocketAddr,
    socket:TcpListener,
    peers_config:Vec<cluster::Controller_Config>,
    peer_sockets:RwLock<HashMap<String,peer>>,


    timer:i32,
    pub leader:bool,
    pub leader_addr:Option<SocketAddr>,
    term:RwLock<u64>,
    
    clients:RwLock<HashMap<SocketAddr,Arc<OwnedWriteHalf>>> 

}


struct peer{
    reader:OwnedReadHalf,
    writer:OwnedWriteHalf,
    id:u64,
    // addr:String
}





impl Controller {
    pub async fn new(controller:Controller_Config,peers_config:Vec<Controller_Config>,isleader:bool)->Result<Self,ConrtollerError>{
        let socket=CreateControllerSocket(controller.port.clone(), controller.ip.clone()).await?;

        let mut map=HashMap::new();
        for peer in peers_config.clone(){
            if peer.id == controller.id {
                continue;
            }
            let (reader,writer,addr,controller_id)= CreatePeerSockets(peer).await?;
            map.insert(addr, peer{
                reader,
                writer,
                id:controller_id,
            });
        }

        Ok(Self{
            id:controller.id,
            port:controller.port,
            ip:controller.ip,
            socket,
            peers_config,
            peer_sockets:RwLock::new(map),
            timer:controller.election_timer,
            leader:isleader,
            leader_addr:None,
            clients:RwLock::new(HashMap::new()),
            term:RwLock::new(0),
        })
    }
    
    pub fn Init(mut self,start_loop_signal:oneshot::Receiver<u8>)->oneshot::Receiver<u8>{
        let (server_setup_sender_sign,server_setup_recv_sign)=oneshot::channel::<u8>();
        
        tokio::spawn(async move{
            let controller=Arc::new(self);
            server_setup_sender_sign.send(1).unwrap();
            start_loop_signal.await.unwrap();
            loop{
                let (stream,client_addr)=controller.socket.accept().await.unwrap();

                let (mut reader,writer)=stream.into_split();
            
                let mut clients=controller.clients.write().await;

                clients.insert(client_addr, Arc::new((writer)));

                let client_controller=Arc::clone(&controller);

                drop(clients);

                tokio::spawn(async move{
                    

                
                    loop {
                        let mut buf = [0u8; 8];


                        match timeout(Duration::from_millis(client_controller.timer as u64),reader.read_exact(&mut buf)).await {
                            Ok(Ok(_)) => {
                                let len = u64::from_be_bytes(buf) as usize;

                                let mut operation_buf = vec![0u8; len];

                                reader.read_exact(&mut operation_buf).await.unwrap();

                                let (command_buf,payload)=client_controller.Simplify(operation_buf);



                            }

                            Ok(Err(e)) => {
                                // Socket/read error
                                println!("Read error: {}", e);
                                break;
                            }

                            Err(_) => {
                                // Timer expired
                                println!("Election timeout for {}",client_controller.id);
                                let mut peer_sockets=client_controller.peer_sockets.write().await;

                                let responses=
                                for (addr, peer)in peer_sockets.iter_mut(){

                                    let mut buf=Vec::new();
                                    let mut final_buf=Vec::new();

                                    let command_buf=b"election";
                                    let len=(buf.len() as u64).to_be_bytes();

                                    final_buf.extend_from_slice(&len);
                                    final_buf.extend_from_slice(command_buf);

                                    let term_buf=(client_controller.read().term+1).to_be_bytes();
                                    let len=(term_buf.len() as u64).to_be_bytes();

                                    buf.extend_from_slice(&len);
                                    buf.extend_from_slice(&term_buf);


                                    
                                    let len=(term_buf.len() as u64).to_be_bytes();
                                    final_buf.extend_from_slice(&len);
                                    final_buf.extend_from_slice(&buf);

                                    peer.writer.write_all(&final_buf).await.unwrap();
                                }
                                

                                break;
                            }
                        }



                        reader.read_exact(&mut buf).await.unwrap();

                        



                    }
                    
                });
            }

        }); 
      
        
        server_setup_recv_sign
        
    }
    
    async fn HandleOperations(&mut self,payload:Vec<u8>){
        let(opeartion_buf,payload)=self.Simplify(payload);
        
        match opeartion_buf.as_slice() {
            b"election"=>{
                let(term_buf,payload)=self.Simplify(payload);
                let (socket_addr_buf,payload)=self.Simplify(payload);
                let term = u64::from_be_bytes(
                    term_buf.as_slice().try_into().unwrap()
                );

                let mut controller_term=self.term.write().await;

                if term < *controller_term {
                    // Candidate is using an old term.
                    // Reject the election.
                    let response = b"reject";
                    let mut buf = Vec::new();

                    // response length
                    buf.extend_from_slice(&(response.len() as u64).to_be_bytes());

                    // response
                    buf.extend_from_slice(response);

                    let mut peers=self.peer_sockets.write().await;
                    let socket=peers.get_mut(&String::from_utf8(socket_addr_buf).unwrap()).unwrap();
                    socket.writer.write_all(&buf).await;
                    // write response
                } 
                else if term > *controller_term {
                    // We have learned about a newer term.
                    *controller_term= term;

                    // Become follower / reset vote state
                    // self.state = State::Follower;
                    // self.voted_for = None;

                    let response = b"accept";
                    let mut buf = Vec::new();

                    // response length
                    buf.extend_from_slice(&(response.len() as u64).to_be_bytes());

                    // response
                    buf.extend_from_slice(response);

                    let mut peers=self.peer_sockets.write().await;
                    let socket=peers.get_mut(&String::from_utf8(socket_addr_buf).unwrap()).unwrap();
                    socket.writer.write_all(&buf).await;


                    // write response
                } 

            }

            _=>{

            }
        }
    }

    fn Simplify(&self,payload: Vec<u8>) -> (Vec<u8>, Vec<u8>) {
        let op_len = 8;

        let operation_buf = payload[0..op_len].to_vec();
        let remaining_buf = payload[op_len..].to_vec();

        (operation_buf, remaining_buf)
    }


}


async fn CreateControllerSocket(port:u32,ip:SocketAddr)->Result<TcpListener, ConrtollerError>{
    let port=port.to_string();
    let port=port.as_str();
    let addr=ip.to_string();
    let addr=addr.as_str();

    let addr=format!("{}:{}",addr,port);
    let socket=TcpListener::bind(addr).await?;

    Ok(socket)
}


async fn CreatePeerSockets(peer_config:Controller_Config)->Result<(OwnedReadHalf,OwnedWriteHalf,String,u64),ConrtollerError>{
    let port=peer_config.port.to_string();
    let port=port.as_str();
    let addr=peer_config.ip.to_string();
    let addr=addr.as_str();

    let addr=format!("{}:{}",addr,port);

    let socket=TcpStream::connect(addr.clone()).await?;

    let (read,write)=socket.into_split();

    Ok((read,write,addr,peer_config.id))
}




struct LeaderController(Controller);


impl LeaderController{
    fn new(controller:Controller)->Self{
        Self(controller)
    }


    fn SendHearBeatToPeers(&self){

    }
}