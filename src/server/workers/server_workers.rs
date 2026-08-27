use std::{error::Error, net::SocketAddr, sync::Arc};

use tokio::sync::{RwLock, mpsc, oneshot};

use crate::server::{self, partition, topic, workers::consumer_worker};

#[derive(Clone, serde::Serialize, serde::Deserialize,Debug)]
pub struct ConsumerAssignment {
    pub topic: Vec<u8>,
    pub group_name: Vec<u8>,
    pub consumer_id: u64,
    pub consumer_addr: SocketAddr,
    pub local_partition: u64,
}

pub fn RequestPool() -> mpsc::Sender<(Arc<RwLock<server::init::server>>,Vec<u8>,Vec<u8>,SocketAddr,i64)> {
    let (sender, mut queue) =mpsc::channel::<(Arc<RwLock<server::init::server>>,Vec<u8>,Vec<u8>,SocketAddr,i64)>(1024);
    let worker_queue =InitWorkers(4);
    tokio::spawn(async move {
        let mut previous_receiver:Option<oneshot::Receiver<bool>> =None;

        while let Some((server,operation,payload,client_addr,req_id)) = queue.recv().await{
            let (signal_sender,signal_receiver ) = oneshot::channel::<bool>();

            let previous =previous_receiver.take();

            previous_receiver =Some(signal_receiver);

            

            if let Err(e) =
                worker_queue
                    .send((
                        server,
                        operation,
                        payload,
                        previous,
                        signal_sender,
                        client_addr,
                        req_id
                    ))
                    .await
            {
                eprintln!("Failed to queue request into producer worker queue: {}",e);
                break;
            }
        }
    });

    sender
}


type WorkerRequest = (
    Arc<RwLock<server::init::server>>,
    Vec<u8>,
    Vec<u8>,
    Option<oneshot::Receiver<bool>>,
    oneshot::Sender<bool>,
    SocketAddr,
    i64
);

fn InitWorkers(worker_count: usize) -> mpsc::Sender<WorkerRequest> {
        let (sender, mut queue) =mpsc::channel::<WorkerRequest>(1024);

        if worker_count == 0 {
            eprintln!("Worker pool cannot have zero workers");
            return sender;
        }

        let mut worker_senders =Vec::new();
        let consumer_reg_queue=consumer_worker::ConsumerReg();

        for _ in 0..worker_count {
            let (worker_sender,mut worker_queue) = mpsc::channel::<WorkerRequest>(256);
            let consumer_req_queue=consumer_reg_queue.clone();
            worker_senders.push(worker_sender);

            tokio::spawn(async move {

                while let Some((
                    server,
                    operation,
                    payload,
                    previous_receiver,
                    signal_sender,
                    client_addr,
                    req_id
                )) = worker_queue.recv().await
                {
                    let result =HandleOperation(&server,operation.clone(),payload).await;

                    
                    match result {
                        Ok(t)=>{
                            match t {
                                Some(OperationResult::Producer {value,partition}) =>{
                                    
                                    if let Some(previous_receiver) = previous_receiver{
                                        if let Err(e) =previous_receiver.await{
                                            eprintln!("Previous request signal failed: {}",e);
                                        }
                                    }

                                    let partition_worker_pool ={
                                        let server_guard =server.read().await;

                                        server_guard.partition_worker_pool.clone()
                                    };

                                    if let Err(e) =
                                        partition_worker_pool.send((Arc::clone(&server),partition,value,client_addr,req_id)).await{
                                        eprintln!("Failed to queue partition write: {}",e);
                                    }

                                    let _ =signal_sender.send(true);
                                }
                                
                                Some(OperationResult::Subscribe { topic, group_name, start_point })=>{
                                    
                                    if let Some(previous_receiver) = previous_receiver{
                                        if let Err(e) =previous_receiver.await{
                                            eprintln!("Previous request signal failed: {}",e);
                                        }
                                    }

                                    if let Err(e) = consumer_req_queue
                                        .send((
                                            Arc::clone(&server),
                                            topic,
                                            group_name,
                                            start_point,
                                            client_addr,
                                            req_id
                                        ))
                                        .await
                                    {
                                        eprintln!(
                                            "Failed to queue consumer registration: {}",
                                            e
                                        );
                                    }
                                    let _ =signal_sender.send(true);
                                }
                                Some(OperationResult::Leader { socket_addr }) => {
                                    if let Some(previous_receiver) = previous_receiver {
                                        if let Err(e) = previous_receiver.await {
                                            eprintln!("Previous request signal failed: {}", e);
                                        }
                                    }

                                    let mut addr_buf = socket_addr.to_string().into_bytes();

                                    let response_writer_signal = {
                                        let server_guard = server.read().await;
                                        server_guard.response_pool.clone()
                                    };

                                    if let Err(e) = response_writer_signal
                                        .send((
                                            Arc::clone(&server),
                                            client_addr,
                                            true,
                                            addr_buf,
                                            req_id
                                        ))
                                        .await
                                    {
                                        eprintln!("Failed to queue leader response: {}", e);
                                    }

                                    let _ = signal_sender.send(true);
                                }
                                None=>{
                                     let response_writer_signal = {
                                let server_guard =server.read().await;
                                server_guard.response_pool.clone()
                            };

                            if let Err(e) =
                                response_writer_signal
                                    .send((
                                        Arc::clone(&server),
                                        client_addr,
                                        true,
                                        Vec::new(),
                                        req_id
                                    ))
                                    .await
                            {
                                eprintln!("Failed to queue response: {}",e);
                            }

                            let _ =signal_sender.send(true);
                                }
                                
                            }
                        }
                        

                        

                        // Ok(None) => {
                        //     let response_writer_signal = {
                        //         let server_guard =server.read().await;
                        //         server_guard.response_pool.clone()
                        //     };


                        //     if let Err(e) =
                        //         response_writer_signal
                        //             .send((
                        //                 Arc::clone(&server),
                        //                 client_addr,
                        //                 true,
                        //                 Vec::new(),
                        //             ))
                        //             .await
                        //     {
                        //         eprintln!("Failed to queue response: {}",e);
                        //     }

                        //     let _ =signal_sender.send(true);
                        // }

                        Err(e) => {
                            let response_writer_signal = {
                                let server_guard =server.read().await;
                                server_guard.response_pool.clone()
                            };


                            eprintln!("HandleOperation failed: {}",e);

                            if let Err(e) =
                                response_writer_signal
                                    .send((
                                        Arc::clone(&server),
                                        client_addr,
                                        false,
                                        e.to_string().into_bytes(),
                                        req_id
                                    ))
                                    .await
                            {
                                eprintln!("Failed to queue error response: {}",e);
                            }

                            let _ =signal_sender.send(true);
                        }
                    }
                }
            });
        }

        
        //dispathcer
        //it has teh the req whic it recived from teh request pool adn dsitrubute for parallel processing 
        tokio::spawn(async move {
            let mut next_worker = 0;

            while let Some(request) =queue.recv().await{
                if worker_senders.is_empty() {
                    eprintln!("No workers available");
                    break;
                }

                if let Err(e) =worker_senders[next_worker].send(request).await{
                    eprintln!("Failed to dispatch request to worker: {}",e);
                    break;
                }

                next_worker =(next_worker + 1)% worker_senders.len();
            }
        });

        sender
    }



enum OperationResult {
    Producer {
        value: Vec<u8>,
        partition: Arc<RwLock<partition::Partition>>,
    },

    Subscribe {
        topic:Vec<u8>,
        group_name: Vec<u8>,
        start_point: Vec<u8>,
    },


    Leader{
        socket_addr: SocketAddr,
    }
}





async fn HandleOperation(server: &Arc<RwLock<server::init::server>>,operation: Vec<u8>,payload: Vec<u8>) -> Result<Option<OperationResult>,Box<dyn std::error::Error + Send + Sync>,> {
    let operation =String::from_utf8(operation)
            .map_err(|e| { format!("Invalid operation UTF-8: {}",e)})?;
    // println!("SErver: opeartio si {} adn paylaod is {:?}",operation,payload);
    match operation.trim() {
        "topic_insert" => {

            // println!("Broker/Server: payload is {:?}",payload);
            let (topic_name_buf,payload,) = Simplify(payload)?;

            

            let (partition_buf,payload) = Simplify(payload)?;

            let partition_no = u64::from_be_bytes(partition_buf.try_into().map_err(|_| "Failed to parse partition count")?,) as usize;

            let (broker_id_buf, _) = Simplify(payload)?;

            let broker_id = u64::from_be_bytes(
                broker_id_buf
                    .try_into()
                    .map_err(|_| "Invalid broker ID")?
            );
            if partition_no == 0 {
                return Err("Partition count cannot be zero".into());
            }

            let topic_map = {
                let server_guard =server.read().await;

                let shard =server_guard.GetShard(&topic_name_buf,server_guard.shard_count);

                let topic_map =server_guard.shard_map.get(&shard)
                .ok_or_else(|| {
                    format!("Shard {} not found",shard.0)
                })?;

                Arc::clone(topic_map)
            };

            let mut topic_map_guard =topic_map.write().await;

            let topic =topic::topic::new(&topic_name_buf,partition_no,broker_id)?;

            topic_map_guard.insert(topic_name_buf,topic,);


            Ok(None)
        }

        "topic_data_insert" => {
            let (topic_name_buf,payload) = Simplify(payload)?;

            let (key_buf,payload) = Simplify(payload)?;

            let (value_buf,payload,) = Simplify(payload)?;

            let (local_partition_id_buf, _) = Simplify(payload)?;

            let local_partition_id = u64::from_be_bytes(
                local_partition_id_buf
                    .try_into()
                    .map_err(|_| "Invalid local partition ID")?
            ) as usize;

            let partition = {
                let server_guard =server.read().await;

                let shard =
                    server_guard.GetShard(&topic_name_buf,server_guard.shard_count,);

                let topic_map =server_guard.shard_map.get(&shard)
                        .ok_or_else(|| {
                            format!("Shard {} not found",shard.0)
                        })?;

                let topic_map_guard =topic_map.read().await;

                let topic =topic_map_guard.get(&topic_name_buf)
                        .ok_or_else(|| {
                            format!(
                                "Topic '{}' does not exist",
                                String::from_utf8_lossy(
                                    &topic_name_buf
                                )
                            )
                        })?;

                let partition_no =topic.partition_no;

                if partition_no == 0 {
                    return Err("Topic has zero partitions".into());
                }

                let key_buf_hash =server_guard.GetHash(&key_buf)as usize;

                let partition_id =key_buf_hash% partition_no;

                let partition =topic.partitions.get(&local_partition_id)
                        .ok_or_else(|| {
                            format!(
                                "Partition {} not found for topic",
                                partition_id
                            )
                        })?;

                println!("partiotn id we sent is {} adn which artion got seledted {:?}",local_partition_id,partition);
                Arc::clone(partition)
            };

            

            Ok(Some(OperationResult::Producer {
                value:value_buf,
                partition,
            }))
        }

        "subscribe"=>{
            let (topic_name_buf,payload) = Simplify(payload)?;

            let (group_name_buf,payload) = Simplify(payload)?;

            let (start_buf,_)=Simplify(payload)?;

            Ok(Some(OperationResult::Subscribe { topic: topic_name_buf, group_name: group_name_buf, start_point: start_buf }))
        }

        "who_leader" => {
            let leader_addr = {
                let server_guard = server.read().await;

                server_guard.leader_socket_addr
            };  


            match leader_addr {
                Some(socket_addr) => {
                    Ok(Some(OperationResult::Leader { socket_addr }))
                }

                None => {
                    // No leader has been elected yet.
                    Ok(None)
                }
            }
        }

        "consumer_assignment"=>{
            let assignments:Vec<ConsumerAssignment> =serde_json::from_slice(&payload)?;
            println!("Server/broker:asssignments   {:?}",assignments);
            
            for assignment in assignments {
                let topic_name_buf=assignment.topic;
                let server_guard =server.read().await;

                let shard =
                    server_guard.GetShard(&topic_name_buf,server_guard.shard_count,);

                let topic_map =server_guard.shard_map.get(&shard)
                        .ok_or_else(|| {
                            format!("Shard {} not found",shard.0)
                        })?;

                let topic_map_guard =topic_map.read().await;

                let topic =topic_map_guard.get(&topic_name_buf)
                        .ok_or_else(|| {
                            format!(
                                "Topic '{}' does not exist",
                                String::from_utf8_lossy(
                                    &topic_name_buf
                                )
                            )
                        })?;

                let local_partition_id =assignment.local_partition as usize;

                

                let partition =topic.partitions.get(&local_partition_id)
                .ok_or_else(|| {
                    format!(
                        "Partition {} not found for topic",
                        local_partition_id
                    )
                })?;

                let partition_guard=partition.read().await;
                let consumers=partition_guard.consumers.write().await;

            

            }


            Ok(None)
        }
       
        unknown => {
            Err(
                format!(
                    "Unknown operation: {}",
                    unknown
                )
                .into()
            )
        }
    }
}


pub fn Simplify(buf: Vec<u8>,) -> Result<(Vec<u8>, Vec<u8>),Box<dyn Error + Send + Sync>,> {
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
