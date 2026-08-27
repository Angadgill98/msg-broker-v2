

use std::{collections::HashMap, error::Error, hash::{DefaultHasher, Hash, Hasher}, net::SocketAddr, sync::Arc};

use rand::RngExt;
use serde::{Deserialize, Serialize};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpStream, tcp::{OwnedReadHalf, OwnedWriteHalf}}, sync::{RwLock, mpsc, oneshot}};

use crate::brokers;


#[derive(Debug)]

#[derive(Clone, Serialize, Deserialize)]
pub struct ConsumerInfo {
    pub consumer_id: u64,
    pub consumer_addr: SocketAddr,
    pub start_point: u64,
}








pub struct client {
    metadata: HashMap<String, HashMap<u64, (SocketAddr, u64)>>,

    brokers_stream:HashMap<SocketAddr,mpsc::Sender<(Vec<u8>, Option<oneshot::Sender<Vec<u8>>>)>>,

    leader_stream: Option<TcpStream>,


    consumers:Arc<RwLock<HashMap<u64,mpsc::Sender<String>>>>,
}


impl client{
    pub async fn new(brokers_config:Vec<brokers::Brokers_config>)->Self{
        let mut map=HashMap::new();
        let (client_bck_sender,client_bck_reciever)=mpsc::channel::<(Vec<u8>,i64)>(1024);
        let responser_queue=client::StartClientBackgroundReader(client_bck_reciever);
        let consumers=Arc::new(RwLock::new(HashMap::new()));
        let consumer_pool_sender=client::StartConsumerPool(Arc::clone(&consumers));

        for config in brokers_config{
            let socket=TcpStream::connect(config.ip.clone()).await.unwrap();
            let (reader,writer)=socket.into_split();
            client::StartBrokerBackgroundReader(reader, client_bck_sender.clone(),consumer_pool_sender.clone());
            let dispatcher=client::StartBrokerRequestDispatcher(writer,responser_queue.clone());
            map.insert(config.ip,dispatcher);
        }

        Self{
            metadata:HashMap::new(),
            brokers_stream:map,
            leader_stream:None,
            consumers
        }

    }

    fn StartBrokerBackgroundReader(mut reader:OwnedReadHalf,client_bck_sender:mpsc::Sender<(Vec<u8>,i64)>,consumer_pool_sender:mpsc::Sender<Vec<u8>>){
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

                {
                    if let Ok((is_consumer_response, payload)) = Simplify(response.clone()) {
                        if is_consumer_response == b"consumer" {
                            consumer_pool_sender.send(payload).await.unwrap();
                            continue;
                        }
                    }
                }
                

                let res_len=(response.len() as u64).to_be_bytes();


                let mut final_buf=Vec::new();
                final_buf.extend_from_slice(&res_len);
                final_buf.extend_from_slice(&response);
                


                let req_id = i64::from_be_bytes(req_id_buf.try_into().unwrap());





                client_bck_sender.send((final_buf,req_id)).await.unwrap();

            }
        });
    }

    fn StartConsumerPool(consumers:Arc<RwLock<HashMap<u64,mpsc::Sender<String>>>>)->mpsc::Sender<Vec<u8>>{
        let consumers=consumers;
        let (consumer_pool_sender,mut queue)=mpsc::channel::<Vec<u8>>(1024);
        tokio::spawn(async move{

            while let Some(response) = queue.recv().await {

            // Get consumer ID
            let (consumer_id_buf, payload) =
                Simplify(response).unwrap();

            let consumer_id = u64::from_be_bytes(
                consumer_id_buf
                    .try_into()
                    .unwrap()
            );

            // Get value
            let (value_buf, _) =
                Simplify(payload).unwrap();

            let value =
                String::from_utf8(value_buf).unwrap();

            // println!(
            //     "Consumer ID: {}, Value: {}",
            //     consumer_id,
            //     value
            // );

            // Find consumer and send value
            let consumer_sender = {
                let guard = consumers.read().await;
                guard.get(&consumer_id).cloned()
            };

            if let Some(sender) = consumer_sender {
                sender.send(value).await.unwrap();
            }
        }


        });
        consumer_pool_sender
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
                            println!("Cleint: response is {:?} adn req id is {:?}",response,req_id);

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

    fn StartBrokerRequestDispatcher(mut writer: OwnedWriteHalf,response_queue: mpsc::Sender<(oneshot::Sender<Vec<u8>>, i64)>,) -> mpsc::Sender<(Vec<u8>, Option<oneshot::Sender<Vec<u8>>>)> {

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

    pub async fn send_topic_data(&mut self,topic: String,key: Option<String>,value: String,) -> Result<(), Box<dyn Error>> {
        let mut buf = Vec::new();
        let mut final_buf = Vec::new();


        let op = b"topic_data_insert";
        let op_len = (op.len() as u64).to_be_bytes();

        final_buf.extend_from_slice(&op_len);
        final_buf.extend_from_slice(op);


        let topic_buf = topic.as_bytes();
        let topic_len = (topic_buf.len() as u64).to_be_bytes();

        buf.extend_from_slice(&topic_len);
        buf.extend_from_slice(topic_buf);
        

        let mut keybuf=Vec::new();
        match key {
            Some(s) => {
                let key_buf = s.as_bytes();
                let key_len = (key_buf.len() as u64).to_be_bytes();
                keybuf=key_buf.to_vec();
                buf.extend_from_slice(&key_len);
                buf.extend_from_slice(key_buf);
            }

            None => {
                buf.extend_from_slice(&0u64.to_be_bytes());
            }
        }

        let value_buf = value.as_bytes();
        let value_len = (value_buf.len() as u64).to_be_bytes();

        buf.extend_from_slice(&value_len);
        buf.extend_from_slice(value_buf);



        let partitions=self.metadata.get(&topic).unwrap();

        let partition_no=partitions.len();

        let global_partitiion_no=self.Hashkey(&keybuf, partition_no);

        let (broker_ip,local_partition_no)=partitions.get(&(global_partitiion_no as u64)).unwrap();

        let sender=self.brokers_stream.get_mut(broker_ip).unwrap();

        let no_buf=local_partition_no.to_be_bytes();

        let len=(no_buf.len() as u64 ).to_be_bytes();

        buf.extend_from_slice(&len);
        buf.extend_from_slice(&no_buf);

        let buf_len = (buf.len() as u64).to_be_bytes();
        final_buf.extend_from_slice(&buf_len);
        final_buf.extend_from_slice(&buf);


        let(response_sender,response_receiver)=oneshot::channel::<Vec<u8>>();

        sender.send((final_buf,Some(response_sender))).await.unwrap();
        let a=response_receiver.await.unwrap();

        println!("res is {:?}",a);

        // Send to server

        Ok(())
    }

    fn Hashkey(&self,topic: &Vec<u8>,partition_count: usize)->usize{
        let mut hasher =DefaultHasher::new();

        topic.hash(&mut hasher);

        let hash = hasher.finish();

        (hash as usize)% partition_count
    }



    pub async fn subscribe(&mut self,topic: String,group_name: String,start_point: usize,) -> Result<(), Box<dyn Error>> {

        let mut buf = Vec::new();
        let mut final_buf = Vec::new();

        // -------------------------------------------------
        // Operation
        // -------------------------------------------------

        let op = b"subscribe";
        let op_len = (op.len() as u64).to_be_bytes();

        final_buf.extend_from_slice(&op_len);
        final_buf.extend_from_slice(op);

        // -------------------------------------------------
        // Topic
        // -------------------------------------------------

        let topic_buf = topic.as_bytes();
        let topic_len = (topic_buf.len() as u64).to_be_bytes();

        buf.extend_from_slice(&topic_len);
        buf.extend_from_slice(topic_buf);

        // -------------------------------------------------
        // Key
        // -------------------------------------------------

        let grp_buf = group_name.as_bytes();
        let grp_len = (grp_buf.len() as u64).to_be_bytes();

        buf.extend_from_slice(&grp_len);
        buf.extend_from_slice(grp_buf);

        // -------------------------------------------------
        // Start point
        // -------------------------------------------------

        let start_point_buf = (start_point as u64).to_be_bytes();

        let start_point_len =
            (start_point_buf.len() as u64).to_be_bytes();

        buf.extend_from_slice(&start_point_len);
        buf.extend_from_slice(&start_point_buf);



        let len=(buf.len() as u64).to_be_bytes();

        final_buf.extend_from_slice(&len);
        final_buf.extend_from_slice(&buf);        
        // -------------------------------------------------
        // Send request
        // -------------------------------------------------

        if let Some(stream) = &mut self.leader_stream {
            stream.write_all(&final_buf).await.unwrap();

            // Read payload length
            let mut len_buf = [0u8; 8];
            stream.read_exact(&mut len_buf).await.unwrap();

            let len = u64::from_be_bytes(len_buf) as usize;

            // Read payload
            let mut payload = vec![0u8; len];
            stream.read_exact(&mut payload).await.unwrap();

            let(consumer_arrangement,payload)=Simplify(payload).unwrap();

            // Deserialize the complete topics map
            let topic_partitions: HashMap<u64,(SocketAddr, u64, ConsumerInfo)> = serde_json::from_slice(&consumer_arrangement).unwrap();

            let(consumer_id_buf,payload)=Simplify(payload).unwrap();
            
            let consumer_id = u64::from_be_bytes(
                consumer_id_buf.try_into().unwrap()
            );

            // println!("Received topics: {:?}", topic_partitions);
            
            
            let (sender,mut receiver)=mpsc::channel::<String>(1024);
            self.consumers.write().await.insert(consumer_id.clone(), sender);





            let op = b"consumer_assignment";
            let op_len = (op.len() as u64).to_be_bytes();

            
            for (global_partition, (broker_ip, local_partition_no, consumer)) in &topic_partitions {
                

                let mut buf=Vec::new();
                let mut final_buf=Vec::new();
                
                    // -------------------------
                    // Operation
                    // -------------------------

                    final_buf.extend_from_slice(&op_len);
                    final_buf.extend_from_slice(op);

                    // -------------------------
                    // Topic
                    // -------------------------

                    buf.extend_from_slice(&topic_len);
                    buf.extend_from_slice(topic_buf);

                    // -------------------------
                    // Group name
                    // -------------------------

                    let group_len = (grp_buf.len() as u64).to_be_bytes();

                    buf.extend_from_slice(&group_len);
                    buf.extend_from_slice(grp_buf);

                    // -------------------------
                    // Start point
                    // -------------------------

                    buf.extend_from_slice(&start_point_len);
                    buf.extend_from_slice(&start_point_buf);

                    // -------------------------
                    // Local partition
                    // -------------------------

                    let partition_buf = local_partition_no.to_be_bytes();
                    let partition_len = (partition_buf.len() as u64).to_be_bytes();

                    buf.extend_from_slice(&partition_len);
                    buf.extend_from_slice(&partition_buf);

                    let consumer_buf=serde_json::to_vec(consumer).unwrap();
                    let len=(consumer_buf.len() as u64).to_be_bytes();

                    buf.extend_from_slice(&len);
                    buf.extend_from_slice(&consumer_buf);

                    // -------------------------
                    // Complete payload
                    // -------------------------

                    let buf_len = (buf.len() as u64).to_be_bytes();

                    final_buf.extend_from_slice(&buf_len);
                    final_buf.extend_from_slice(&buf);

                    let sender = self.brokers_stream
                        .get_mut(broker_ip)
                        .unwrap();

                    sender.send((final_buf, None)).await.unwrap();

            }
         
         

            //this is req when we wan tht subscriber to actiberly lsiten to msg
            // tokio::spawn(async move {
            //     while let Some(payload) = receiver.recv().await {
            //         println!("Received: {:?}", payload);
            //     }
            // });

        };

        
        

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
