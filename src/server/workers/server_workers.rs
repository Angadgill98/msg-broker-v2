use std::{error::Error, net::SocketAddr, sync::Arc};

use tokio::sync::{RwLock, mpsc, oneshot};

use crate::server::{self, partition, topic, workers::consumer_worker};

pub fn RequestPool() -> mpsc::Sender<(Arc<RwLock<server::init::server>>,Vec<u8>,Vec<u8>,SocketAddr)> {
    let (sender, mut queue) =mpsc::channel::<(Arc<RwLock<server::init::server>>,Vec<u8>,Vec<u8>,SocketAddr,)>(1024);
    let worker_queue =InitWorkers(4);
    tokio::spawn(async move {
        let mut previous_receiver:Option<oneshot::Receiver<bool>> =None;

        while let Some((server,operation,payload,client_addr,)) = queue.recv().await{
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
                                        partition_worker_pool.send((Arc::clone(&server),partition,value,client_addr)).await{
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
                                None=>{
                                     let response_writer_signal = {
                                let server_guard =server.read().await;
                                server_guard.response_pool.clone()
                            };
                            println!("asdasdas");

                            if let Err(e) =
                                response_writer_signal
                                    .send((
                                        Arc::clone(&server),
                                        client_addr,
                                        true,
                                        Vec::new(),
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
}





async fn HandleOperation(server: &Arc<RwLock<server::init::server>>,operation: Vec<u8>,payload: Vec<u8>) -> Result<Option<OperationResult>,Box<dyn std::error::Error + Send + Sync>,> {
    let operation =String::from_utf8(operation)
            .map_err(|e| { format!("Invalid operation UTF-8: {}",e)})?;

    match operation.trim() {
        "topic_insert" => {
            let (topic_name_buf,payload,) = Simplify(payload)?;

            if payload.len() != 8 {
                return Err(
                    format!("Invalid partition count payload: expected 8 bytes, got {}",payload.len())
                    .into()
                );
            }

            let partition_no =u64::from_be_bytes(
                payload.try_into().map_err(|_| {"Failed to parse partition count"})?,
            ) as usize;

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

            let topic =topic::topic::new(&topic_name_buf,partition_no,)?;

            topic_map_guard.insert(topic_name_buf,topic,);

            Ok(None)
        }

        "topic_data_insert" => {
            let (topic_name_buf,payload) = Simplify(payload)?;

            let (key_buf,payload) = Simplify(payload)?;

            let (value_buf,_,) = Simplify(payload)?;

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

                let partition =topic.partitions.get(&partition_id)
                        .ok_or_else(|| {
                            format!(
                                "Partition {} not found for topic",
                                partition_id
                            )
                        })?;

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
