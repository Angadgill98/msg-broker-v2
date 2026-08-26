// use std::collections::HashMap;
// use std::error::Error;
// use std::net::SocketAddr;

// use tokio::io::{AsyncReadExt, AsyncWriteExt};
// use tokio::net::TcpStream;
// use tokio::net::tcp::OwnedWriteHalf;
// use tokio::sync::mpsc::{self, Receiver, Sender};
// use tokio::sync::watch;

// use crate::brokers;
// use crate::consumer::inti::consumer;
// use crate::producer::init::producer;





// pub struct client{
//     // socket:OwnedWriteHalf,
//     consumer:consumer,
//     producer:producer,

//     request_queue_signal:Sender<Vec<u8>>,
//     response_signal: watch::Receiver<Vec<u8>>,
//     consumer_socket:Sender<Vec<u8>>,

//     brokers_sockets:HashMap<SocketAddr,TcpStream>,
//     leader_controller:Option<TcpStream>,

//     data:HashMap<String,SocketAddr>

// }




// impl client {
//     pub async fn init(brokers_config:Vec<brokers::Brokers_config>) -> Result<Self, Box<dyn Error>> {
//         let socket = client::CreateSocket().await?;

//         let (mut reader, writer) = socket.into_split();

//         let (response_tx, response_rx) =watch::channel(Vec::<u8>::new());

//         let response_tx_clone = response_tx.clone();

    
//         tokio::spawn(async move {
//             loop {
//                 // -------------------------
//                 // Read ACK
//                 // -------------------------
//                 let mut ack_buf = [0u8; 1];

//                 if let Err(e) = reader.read_exact(&mut ack_buf).await {
//                     eprintln!("Server connection closed: {}", e);
//                     break;
//                 }

//                 let ack = ack_buf[0] == 1;

//                 // -------------------------
//                 // Read response length
//                 // -------------------------
//                 let mut len_buf = [0u8; 8];

//                 if let Err(e) = reader.read_exact(&mut len_buf).await {
//                     eprintln!("Failed to read response length: {}", e);
//                     break;
//                 }

//                 let len = u64::from_be_bytes(len_buf) as usize;

//                 // -------------------------
//                 // Read response
//                 // -------------------------
//                 let mut response = vec![0u8; len];

//                 if let Err(e) = reader.read_exact(&mut response).await {
//                     eprintln!("Failed to read response: {}", e);
//                     break;
//                 }

//                 // -------------------------
//                 // Handle response
//                 // -------------------------

//                 if ack {
//                     println!("ACK recived");
//                     println!("Response: {:?}", response);
//                     let _ =response_tx_clone.send(response);
//                 } else {
//                     println!(
//                         "Request failed: {}",
//                         String::from_utf8_lossy(&response)
//                     );
//                 }
//             }
//         });


//         let consumer_socket: TcpStream=client::CreateSocket().await?;

//         let(mut consumer_reader,consumer_writer)=consumer_socket.into_split();

//         let (consumer_response_tx, consumer_response_rx) =mpsc::channel(1024);

//         tokio::spawn(async move {
//             loop {
//                 // -------------------------
//                 // Read ACK
//                 // -------------------------
//                 let mut ack_buf = [0u8; 1];

//                 if let Err(e) = consumer_reader.read_exact(&mut ack_buf).await {
//                     eprintln!("Server connection closed: {}", e);
//                     break;
//                 }

//                 let ack = ack_buf[0] == 1;

//                 // -------------------------
//                 // Read response length
//                 // -------------------------
//                 let mut len_buf = [0u8; 8];

//                 if let Err(e) = consumer_reader.read_exact(&mut len_buf).await {
//                     eprintln!("Failed to read response length: {}", e);
//                     break;
//                 }

//                 let len = u64::from_be_bytes(len_buf) as usize;

//                 // -------------------------
//                 // Read response
//                 // -------------------------
//                 let mut response = vec![0u8; len];

//                 if let Err(e) = consumer_reader.read_exact(&mut response).await {
//                     eprintln!("Failed to read response: {}", e);
//                     break;
//                 }

//                 // -------------------------
//                 // Handle response
//                 // -------------------------

//                 if ack {
//                     println!("ACK recived");
//                     println!("Response: {:?}", response);
//                 } else {
//                     println!(
//                         "Request failed: {}",
//                         String::from_utf8_lossy(&response)
//                     );
//                 }
//             }
//         });

      

//         let mut map=HashMap::new();

//         for config in brokers_config{
//             let socket=TcpStream::connect(config.ip.clone()).await.unwrap();
//             map.insert(config.ip, socket);
//         }



//         Ok(Self {
            
//             consumer: consumer::new(),
//             producer: producer::new(),
//             request_queue_signal:client::RequestQueue(writer),
//             response_signal:response_rx,
//             consumer_socket:consumer_response_tx,

//             brokers_sockets:map,
//             leader_controller:None,

//             data:HashMap::new()

//         })
//     }

    
//     fn RequestQueue(mut stream: OwnedWriteHalf) -> Sender<Vec<u8>> {
//         let (sender, mut receiver) = mpsc::channel::<Vec<u8>>(1024);

//         tokio::spawn(async move {
//             const MAX_BATCH_SIZE: usize = 64 * 1024;
//             const MAX_REQUESTS: u64 = 100;
//             const BATCH_TIMEOUT: std::time::Duration =
//                 std::time::Duration::from_millis(1);

//             let mut buffer = Vec::new();
//             let mut request_count: u64 = 0;

//             loop {
//                 let request = if request_count == 0 {
//                     match receiver.recv().await {
//                         Some(request) => request,
//                         None => break,
//                     }
//                 } else {
//                     match tokio::time::timeout(
//                         BATCH_TIMEOUT,
//                         receiver.recv(),
//                     )
//                     .await
//                     {
//                         Ok(Some(request)) => request,

//                         Ok(None) => {
//                             break;
//                         }

//                         Err(_) => {
//                             // -----------------------------
//                             // Timeout -> flush batch
//                             // -----------------------------

//                             let buffer_len =
//                                 buffer.len() as u64;

//                             let mut batch =
//                                 Vec::with_capacity(
//                                     16 + buffer.len()
//                                 );

//                             // Number of requests
//                             batch.extend_from_slice(
//                                 &request_count.to_be_bytes()
//                             );

//                             // Total buffer length
//                             batch.extend_from_slice(
//                                 &buffer_len.to_be_bytes()
//                             );

//                             // All requests
//                             batch.extend_from_slice(&buffer);

//                             if let Err(e) =
//                                 stream.write_all(&batch).await
//                             {
//                                 eprintln!(
//                                     "Failed to send batch: {}",
//                                     e
//                                 );
//                                 break;
//                             }

//                             buffer.clear();
//                             request_count = 0;

//                             continue;
//                         }
//                     }
//                 };

//                 // -----------------------------
//                 // Add request to batch
//                 // -----------------------------

//                 let request_len =
//                     (request.len() as u64).to_be_bytes();

//                 buffer.extend_from_slice(&request_len);
//                 buffer.extend_from_slice(&request);

//                 request_count += 1;

//                 // -----------------------------
//                 // Flush if batch is full
//                 // -----------------------------

//                 if buffer.len() >= MAX_BATCH_SIZE
//                     || request_count >= MAX_REQUESTS
//                 {
//                     let buffer_len =
//                         buffer.len() as u64;

//                     let mut batch =
//                         Vec::with_capacity(
//                             16 + buffer.len()
//                         );

//                     // Number of requests
//                     batch.extend_from_slice(
//                         &request_count.to_be_bytes()
//                     );

//                     // Total buffer length
//                     batch.extend_from_slice(
//                         &buffer_len.to_be_bytes()
//                     );

//                     // All requests
//                     batch.extend_from_slice(&buffer);

//                     // Send
//                     if let Err(e) =
//                         stream.write_all(&batch).await
//                     {
//                         eprintln!(
//                             "Failed to send batch: {}",
//                             e
//                         );
//                         break;
//                     }

//                     // Reset
//                     buffer.clear();
//                     request_count = 0;
//                 }
//             }

//             // -----------------------------
//             // Flush remaining requests
//             // -----------------------------

//             if request_count > 0 {
//                 let buffer_len =
//                     buffer.len() as u64;

//                 let mut batch =
//                     Vec::with_capacity(
//                         16 + buffer.len()
//                     );

//                 // Number of requests
//                 batch.extend_from_slice(
//                     &request_count.to_be_bytes()
//                 );

//                 // Total buffer length
//                 batch.extend_from_slice(
//                     &buffer_len.to_be_bytes()
//                 );

//                 // All requests
//                 batch.extend_from_slice(&buffer);

//                 if let Err(e) =
//                     stream.write_all(&batch).await
//                 {
//                     eprintln!(
//                         "Failed to send final batch: {}",
//                         e
//                     );
//                 }
//             }
//         });

//         sender
//     }

    
//     pub async fn CreateSocket() -> Result<TcpStream, Box<dyn Error>> {
//         let addr = std::env::var("server_addr")
//             .map_err(|_| "Environment variable 'server_addr' not defined")?;

//         let stream = TcpStream::connect(addr).await?;

//         Ok(stream)
//     }

//     // pub async fn insert_topic(&mut self,topic_name: String,partition_no: u64) -> Result<(), Box<dyn Error>> {
        
//     //     self.leader_controller
        
//     //     let op = "topic_insert".as_bytes();
//     //     let op_len = (op.len() as u64).to_be_bytes();

//     //     let topic_buf = topic_name.as_bytes();
//     //     let topic_len = (topic_buf.len() as u64).to_be_bytes();

//     //     let partition_buf = partition_no.to_be_bytes();

//     //     let mut buf = Vec::new();

//     //     buf.extend_from_slice(&op_len);
//     //     buf.extend_from_slice(op);

//     //     buf.extend_from_slice(&topic_len);
//     //     buf.extend_from_slice(topic_buf);

//     //     buf.extend_from_slice(&partition_buf);

//     //     // Total payload length
//     //     // let buf_len = (buf.len() as u64).to_be_bytes();

//     //     // let mut final_buf = Vec::new();

//     //     // final_buf.extend_from_slice(&buf_len);
//     //     // final_buf.extend_from_slice(&buf);

//     //     // Send to server
//     //     self.request_queue_signal.send(buf).await?;

//     //     let res=self.response_signal.borrow().clone();

//     //     self.response_signal.changed().await?;

//     //     let res = self.response_signal.borrow().clone();
//     //     println!("res came ");
//     //     Ok(())
//     // }


//     pub async fn insert_topic(&mut self,topic_name: String,partition_no: u64,) -> Result<(), Box<dyn Error>> {

//     // Make sure we have a leader connection.
//     if self.leader_controller.is_none() {
//         self.get_leader_stream().await?;
//     }

//     // At this point leader_controller MUST exist.
//     let leader = self
//         .leader_controller
//         .as_mut()
//         .ok_or("Leader controller not available")?;

//     let op = b"topic_insert";
//     let op_len = (op.len() as u64).to_be_bytes();

//     let topic_buf = topic_name.as_bytes();
//     let topic_len = (topic_buf.len() as u64).to_be_bytes();

//     let partition_buf = partition_no.to_be_bytes();

//     let mut buf = Vec::new();

//     buf.extend_from_slice(&op_len);
//     buf.extend_from_slice(op);

//     buf.extend_from_slice(&topic_len);
//     buf.extend_from_slice(topic_buf);

//     buf.extend_from_slice(&partition_buf);

//     leader.write_all(&buf).await?;

//     // Wait for response
//     self.response_signal.changed().await?;

//     let res = self.response_signal.borrow().clone();

//     println!("res came: {:?}", res);

//     Ok(())
// }

//     pub async fn send_topic_data(
//         &mut self,
//         topic: String,
//         key: Option<String>,
//         value: String,
//     ) -> Result<(), Box<dyn Error>> {
//         let mut buf = Vec::new();

//         let op = b"topic_data_insert";
//         let op_len = (op.len() as u64).to_be_bytes();

//         buf.extend_from_slice(&op_len);
//         buf.extend_from_slice(op);

//         let topic_buf = topic.as_bytes();
//         let topic_len = (topic_buf.len() as u64).to_be_bytes();

//         buf.extend_from_slice(&topic_len);
//         buf.extend_from_slice(topic_buf);

//         match key {
//             Some(s) => {
//                 let key_buf = s.as_bytes();
//                 let key_len = (key_buf.len() as u64).to_be_bytes();

//                 buf.extend_from_slice(&key_len);
//                 buf.extend_from_slice(key_buf);
//             }

//             None => {
//                 buf.extend_from_slice(&0u64.to_be_bytes());
//             }
//         }

//         let value_buf = value.as_bytes();
//         let value_len = (value_buf.len() as u64).to_be_bytes();

//         buf.extend_from_slice(&value_len);
//         buf.extend_from_slice(value_buf);

//         // Total payload length
//         // let buf_len = (buf.len() as u64).to_be_bytes();

//         // let mut final_buf = Vec::with_capacity(8 + buf.len());

//         // final_buf.extend_from_slice(&buf_len);
//         // final_buf.extend_from_slice(&buf);

//         // Send to server
//         self.request_queue_signal.send(buf).await?;

//         Ok(())
//     }

//     pub async fn subscribe(
//         &mut self,
//         topic: String,
//         group_name: String,
//         start_point: usize,
//     ) -> Result<(), Box<dyn Error>> {

//         let mut buf = Vec::new();

//         // -------------------------------------------------
//         // Operation
//         // -------------------------------------------------

//         let op = b"subscribe";
//         let op_len = (op.len() as u64).to_be_bytes();

//         buf.extend_from_slice(&op_len);
//         buf.extend_from_slice(op);

//         // -------------------------------------------------
//         // Topic
//         // -------------------------------------------------

//         let topic_buf = topic.as_bytes();
//         let topic_len = (topic_buf.len() as u64).to_be_bytes();

//         buf.extend_from_slice(&topic_len);
//         buf.extend_from_slice(topic_buf);

//         // -------------------------------------------------
//         // Key
//         // -------------------------------------------------

//         let grp_buf = group_name.as_bytes();
//         let grp_len = (grp_buf.len() as u64).to_be_bytes();

//         buf.extend_from_slice(&grp_len);
//         buf.extend_from_slice(grp_buf);

//         // -------------------------------------------------
//         // Start point
//         // -------------------------------------------------

//         let start_point_buf = (start_point as u64).to_be_bytes();

//         let start_point_len =
//             (start_point_buf.len() as u64).to_be_bytes();

//         buf.extend_from_slice(&start_point_len);
//         buf.extend_from_slice(&start_point_buf);

//         // -------------------------------------------------
//         // Send request
//         // -------------------------------------------------

//         self.request_queue_signal
//             .send(buf)
//             .await?;

//         Ok(())
//     }

//     pub async fn get_leader_stream(&mut self,) -> Result<(), Box<dyn Error>> {

//         loop {
//             // Try every broker we currently know.
//             for (_broker_addr, broker_stream) in self.brokers_sockets.iter_mut() {

//                 let command = b"who_leader";

//                 let mut buf = Vec::new();

//                 // command length
//                 buf.extend_from_slice(&(command.len() as u64).to_be_bytes());

//                 // command
//                 buf.extend_from_slice(command);

//                 // payload length = 0
//                 buf.extend_from_slice(&0u64.to_be_bytes());

//                 // Ask broker
//                 if let Err(e) = broker_stream.write_all(&buf).await {
//                     eprintln!("Failed to ask broker for leader: {}", e);
//                     continue;
//                 }

//                 // Read address length
//                 let mut len_buf = [0u8; 8];

//                 if let Err(e) = broker_stream.read_exact(&mut len_buf).await {
//                     eprintln!("Failed to read leader address length: {}", e);
//                     continue;
//                 }

//                 let addr_len = u64::from_be_bytes(len_buf) as usize;

//                 // Broker doesn't know leader yet
//                 if addr_len == 0 {
//                     continue;
//                 }

//                 // Read address
//                 let mut addr_buf = vec![0u8; addr_len];

//                 if let Err(e) = broker_stream.read_exact(&mut addr_buf).await {
//                     eprintln!("Failed to read leader address: {}", e);
//                     continue;
//                 }

//                 let leader_addr = match String::from_utf8(addr_buf)
//                     .ok()
//                     .and_then(|s| s.parse::<SocketAddr>().ok())
//                 {
//                     Some(addr) => addr,

//                     None => {
//                         eprintln!("Broker returned invalid leader address");
//                         continue;
//                     }
//                 };

//                 println!("Client found leader at {}", leader_addr);

//                 // Connect directly to the leader controller.
//                 match TcpStream::connect(leader_addr).await {
//                     Ok(stream) => {
//                         self.leader_controller = Some(stream);

//                         println!(
//                             "Client connected to leader controller {}",
//                             leader_addr
//                         );

//                         return Ok(());
//                     }

//                     Err(e) => {
//                         eprintln!(
//                             "Failed to connect to leader {}: {}",
//                             leader_addr, e
//                         );

//                         continue;
//                     }
//                 }
//             }

//             // Nobody knows the leader yet.
//             println!("No leader found, retrying...");

//             tokio::time::sleep(
//                 tokio::time::Duration::from_millis(1000)
//             ).await;
//         }
//     }

// }

use std::{collections::HashMap, error::Error, net::SocketAddr};

use rand::RngExt;
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpStream, tcp::{OwnedReadHalf, OwnedWriteHalf}}, sync::{mpsc, oneshot}};

use crate::brokers;










pub struct client {
    metadata: HashMap<String, HashMap<u64, (SocketAddr, u64)>>,

    brokers_stream:HashMap<SocketAddr,mpsc::Sender<(Vec<u8>, Option<oneshot::Sender<Vec<u8>>>)>>,

    leader_stream: Option<TcpStream>,
}


impl client{
    pub async fn new(brokers_config:Vec<brokers::Brokers_config>)->Self{
        let mut map=HashMap::new();
        let (client_bck_sender,client_bck_reciever)=mpsc::channel::<(Vec<u8>,i64)>(1024);
        let responser_queue=client::StartClientBackgroundReader(client_bck_reciever);

        for config in brokers_config{
            let socket=TcpStream::connect(config.ip.clone()).await.unwrap();
            let (reader,writer)=socket.into_split();
            client::StartBrokerBackgroundReader(reader, client_bck_sender.clone());
            let dispatcher=client::StartBrokerRequestDispatcher(writer,responser_queue.clone());
            map.insert(config.ip,dispatcher);
        }

        Self{
            metadata:HashMap::new(),
            brokers_stream:map,
            leader_stream:None,
        }

    }

    fn StartBrokerBackgroundReader(mut reader:OwnedReadHalf,client_bck_sender:mpsc::Sender<(Vec<u8>,i64)>){
        tokio::spawn(async move{
            loop {


                let mut req_id_len_buf: [u8; 8]=[0u8;8];
                reader.read_exact(&mut req_id_len_buf).await.unwrap();


                let len = u64::from_be_bytes(req_id_len_buf) as usize;

                
                let mut req_id_buf = vec![0u8; len];
                reader.read_exact(&mut req_id_buf).await.unwrap();

                let mut ack=[0u8;1];
                reader.read_exact(&mut ack).await.unwrap();

                let mut res_len: [u8; 8]=[0u8;8];
                reader.read_exact(&mut res_len).await.unwrap();


                let len = u64::from_be_bytes(res_len) as usize;

                
                let mut response = vec![0u8; len];
                reader.read_exact(&mut response).await.unwrap();

                // println!("Broker: response received {:?}",response);

                let mut final_buf=Vec::new();
                final_buf.extend_from_slice(&res_len);
                final_buf.extend_from_slice(&response);
                


                let req_id = i64::from_be_bytes(req_id_buf.try_into().unwrap());





                client_bck_sender.send((final_buf,req_id)).await.unwrap();

            }
        });
    }

    fn StartClientBackgroundReader(mut client_bck_reciever: mpsc::Receiver<(Vec<u8>, i64)>) -> mpsc::Sender<(oneshot::Sender<Vec<u8>>, i64)> {

        let (response_reader, mut queue) =
            mpsc::channel::<(oneshot::Sender<Vec<u8>>, i64)>(1024);

        tokio::spawn(async move {

            let mut waiting =
                std::collections::HashMap::<
                    i64,
                    oneshot::Sender<Vec<u8>>
                >::new();

            loop {
                tokio::select! {

                    Some((sender, req_id)) = queue.recv() => {
                        waiting.insert(req_id, sender);
                    }

                    Some((response, req_id)) =client_bck_reciever.recv() => {
                        if let Some(sender) =waiting.remove(&req_id){
                            // println!("Cleint: response is {:?} adn req id is {:?}",response,req_id);

                            if let Err(_) = sender.send(response) {
                                eprintln!(
                                    "Failed to send response to request"
                                );
                            }
                        } else {
                            eprintln!(
                                "Response received but no request is waiting"
                            );
                        }

                    }

                

                    else => {
                        break;
                    }
                }
            }
        });

        response_reader
    }

    fn StartBrokerRequestDispatcher(mut writer: OwnedWriteHalf,response_queue: mpsc::Sender<(oneshot::Sender<Vec<u8>>, i64)>) -> mpsc::Sender<(Vec<u8>, Option<oneshot::Sender<Vec<u8>>>)> {

        let (dispatcher, mut queue) =
            mpsc::channel::<(Vec<u8>, Option<oneshot::Sender<Vec<u8>>>)>(1024);

        tokio::spawn(async move {

            const MAX_BATCH_SIZE: usize = 64 * 1024;
            const MAX_REQUESTS: usize = 100;
            const BATCH_TIMEOUT: std::time::Duration =
                std::time::Duration::from_millis(1);

            let mut buffer = Vec::new();
            let mut request_count = 0usize;
            let mut req_id: i64 = 0;

            loop {

                let (request, response_send_to_client_bck_reader) =
                    if request_count == 0 {

                        match queue.recv().await {
                            Some((request, response_send_to_client_bck_reader)) =>
                                (request, response_send_to_client_bck_reader),

                            None => break,
                        }

                    } else {

                        match tokio::time::timeout(
                            BATCH_TIMEOUT,
                            queue.recv(),
                        ).await {

                            Ok(Some((request, response_send_to_client_bck_reader))) =>
                                (request, response_send_to_client_bck_reader),

                            Ok(None) => break,

                            Err(_) => {

                                // -----------------------------------------
                                // Create complete batch
                                //
                                // [request_count]
                                // [batch_length]
                                // [requests]
                                // -----------------------------------------

                                let batch_count =
                                    (request_count as u64).to_be_bytes();

                                let batch_len =
                                    (buffer.len() as u64).to_be_bytes();

                                let mut batch =
                                    Vec::with_capacity(16 + buffer.len());

                                batch.extend_from_slice(&batch_count);
                                batch.extend_from_slice(&batch_len);
                                batch.extend_from_slice(&buffer);
                                    // println!("Client: req being sent {:?}",batch);

                                if let Err(e) =
                                    writer.write_all(&batch).await
                                {
                                    eprintln!(
                                        "Failed to send batch: {}",
                                        e
                                    );
                                    break;
                                }

                                buffer.clear();
                                request_count = 0;

                                continue;
                            }
                        }
                    };

                // -----------------------------------------
                // Current request gets an ID
                // -----------------------------------------

                let current_req_id = rand::rng().random();

                // -----------------------------------------
                // If this request expects a response,
                // register its response sender with the ID.
                // -----------------------------------------

                match response_send_to_client_bck_reader {

                    Some(res_signal) => {

                        if let Err(e) =
                            response_queue
                                .send((res_signal, current_req_id))
                                .await
                        {
                            eprintln!(
                                "Failed to queue response signal: {}",
                                e
                            );
                        }
                    }

                    None => {
                        // No response expected.
                    }
                }

                // -----------------------------------------
                // Add request ID to request
                //
                // [req_id_len: 8]
                // [req_id: 8]
                // [request]
                // -----------------------------------------

                let req_id_bytes =
                    current_req_id.to_be_bytes();

                let req_id_len =
                    (req_id_bytes.len() as u64).to_be_bytes();

                buffer.extend_from_slice(&req_id_len);
                buffer.extend_from_slice(&req_id_bytes);
                buffer.extend_from_slice(&request);

                request_count += 1;

                // -----------------------------------------
                // Increment ID for EVERY request
                // -----------------------------------------

                req_id += 1;

                // -----------------------------------------
                // Flush batch
                // -----------------------------------------

                if buffer.len() >= MAX_BATCH_SIZE
                    || request_count >= MAX_REQUESTS
                {
                    let batch_count =
                        (request_count as u64).to_be_bytes();

                    let batch_len =
                        (buffer.len() as u64).to_be_bytes();

                    let mut batch =
                        Vec::with_capacity(16 + buffer.len());

                    batch.extend_from_slice(&batch_count);
                    batch.extend_from_slice(&batch_len);
                    batch.extend_from_slice(&buffer);

                    if let Err(e) =
                        writer.write_all(&batch).await
                    {
                        eprintln!(
                            "Failed to send batch: {}",
                            e
                        );
                        break;
                    }

                    buffer.clear();
                    request_count = 0;
                }
            }

            // -----------------------------------------
            // Flush remaining requests
            // -----------------------------------------

            if !buffer.is_empty() {

                let batch_count =
                    (request_count as u64).to_be_bytes();

                let batch_len =
                    (buffer.len() as u64).to_be_bytes();

                let mut batch =
                    Vec::with_capacity(16 + buffer.len());

                batch.extend_from_slice(&batch_count);
                batch.extend_from_slice(&batch_len);
                batch.extend_from_slice(&buffer);

                if let Err(e) =
                    writer.write_all(&batch).await
                {
                    eprintln!(
                        "Failed to send final batch: {}",
                        e
                    );
                }
            }
        
        });

        dispatcher
    }

    async fn get_leader_stream(&mut self,) -> Result<(), Box<dyn Error>> {

        if let Some(_) = &self.leader_stream {
            return Ok(());
        }
        loop {
            // Try every broker we currently know.
            for (_broker_addr, broker_stream) in self.brokers_stream.iter_mut() {

                let command = b"who_leader";

                let mut buf = Vec::new();

                // command length
                buf.extend_from_slice(&(command.len() as u64).to_be_bytes());

                // command
                buf.extend_from_slice(command);

                // payload length = 0
                buf.extend_from_slice(&0u64.to_be_bytes());

                let(response_sender,response_receiver)=oneshot::channel::<Vec<u8>>();

                // Ask broker
                if let Err(e) = broker_stream.send((buf,Some(response_sender))).await {
                    eprintln!("Failed to ask broker for leader: {}", e);
                    continue;
                }

                let response=response_receiver.await.unwrap();
                if response.is_empty() {
                    // No response data
                    continue;
                }

                if response.len() < 8 {
                    eprintln!("Response too small to contain address length");
                    continue;
                }

                let mut pos = 0;

                // -------------------------
                // Address length
                // -------------------------

                let mut addr_len_buf = [0u8; 8];

                addr_len_buf.copy_from_slice(&response[pos..pos + 8]);
                pos += 8;

                let addr_len =
                    u64::from_be_bytes(addr_len_buf) as usize;

                // Broker doesn't know leader yet
                if addr_len == 0 {
                    continue;
                }

                // Make sure address actually exists in the response
                if response.len() < pos + addr_len {
                    eprintln!(
                        "Invalid response: address length {} but only {} bytes available",
                        addr_len,
                        response.len() - pos
                    );
                    continue;
                }

                // -------------------------
                // Address
                // -------------------------

                let addr_buf = &response[pos..pos + addr_len];

                let leader_addr =
                    match std::str::from_utf8(addr_buf)
                        .ok()
                        .and_then(|s| s.parse::<SocketAddr>().ok())
                    {
                        Some(addr) => addr,

                        None => {
                            eprintln!("Broker returned invalid leader address");
                            continue;
                        }
                    };

                println!("Client found leader at {}", leader_addr);

                // -------------------------
                // Connect to leader
                // -------------------------

                match TcpStream::connect(leader_addr).await {
                    Ok(stream) => {
                        self.leader_stream = Some(stream);

                        println!(
                            "Client connected to leader controller {}",
                            leader_addr
                        );

                        return Ok(());
                    }

                    Err(e) => {
                        eprintln!(
                            "Failed to connect to leader {}: {}",
                            leader_addr,
                            e
                        );

                        continue;
                    }
                }
            }
            // Nobody knows the leader yet.
            println!("No leader found, retrying...");

            tokio::time::sleep(
                tokio::time::Duration::from_millis(1000)
            ).await;
        }
    }

    pub async fn insert_topic(&mut self,topic_name: String,partition_no: u64) -> Result<(), Box<dyn Error>> {
        
        match self.get_leader_stream().await {
            Ok(a)=>{

            }
            Err(e)=>{
                println!("Client:Failed to get leader while inserting topic  {}",e);
                return Ok(()) ;
            }

        }

        
        let mut final_buf = Vec::new();
        let mut buf = Vec::new();

        


        let op = "create_topic".as_bytes();
        let op_len = (op.len() as u64).to_be_bytes();

        final_buf.extend_from_slice(&op_len);
        final_buf.extend_from_slice(&op);


        let topic_buf = topic_name.as_bytes();
        let topic_len = (topic_buf.len() as u64).to_be_bytes();

        let partition_buf = partition_no.to_be_bytes();
        let partition_len=(partition_buf.len() as u64).to_be_bytes();
        buf.extend_from_slice(&topic_len);
        buf.extend_from_slice(topic_buf);

        buf.extend_from_slice(&partition_len);
        buf.extend_from_slice(&partition_buf);

        let buf_len = (buf.len() as u64).to_be_bytes();


        final_buf.extend_from_slice(&buf_len);
        final_buf.extend_from_slice(&buf);

        if let Some(stream) = &mut self.leader_stream {
            // Send request
            stream.write_all(&final_buf).await.unwrap();

            // ------------------------------------
            // Read JSON length
            // ------------------------------------

            let mut len_buf = [0u8; 8];

            stream
                .read_exact(&mut len_buf)
                .await
                .unwrap();

            let buf_len = u64::from_be_bytes(len_buf) as usize;

            // ------------------------------------
            // Read JSON data
            // ------------------------------------

            let mut buf = vec![0u8; buf_len];

            stream
                .read_exact(&mut buf)
                .await
                .unwrap();

            // ------------------------------------
            // Deserialize partition mapping
            // ------------------------------------

            let partition_mapping:
                std::collections::HashMap<u64, (std::net::SocketAddr, u64)> =
                serde_json::from_slice(&buf)
                    .map_err(|e| format!("Failed to deserialize partition mapping: {}", e))
                    .unwrap();

            // println!("Partition mapping: {:?}", partition_mapping);


            self.metadata.insert(topic_name, partition_mapping);
        }

        Ok(())
    }


    
}




fn Simplify(buf: Vec<u8>,) -> Result<(Vec<u8>, Vec<u8>),Box<dyn Error + Send + Sync>,> {
    if buf.len() < 8 {
        return Err(
            format!(
                "Buffer too short: {} bytes, expected at least 8",
                buf.len()
            )
            .into()
        );
    }

    let len =
        u64::from_be_bytes(
            buf[..8]
                .try_into()
                .map_err(|_| {
                    "Failed to read length"
                })?,
        ) as usize;

    let end =
        8usize
            .checked_add(len)
            .ok_or(
                "Length overflow"
            )?;

    if end > buf.len() {
        return Err(
            format!(
                "Invalid buffer length: declared {}, available {}",
                len,
                buf.len().saturating_sub(8)
            )
            .into()
        );
    }

    let value =
        buf[8..end].to_vec();

    let remaining =
        buf[end..].to_vec();

    Ok((
        value,
        remaining,
    ))
}
