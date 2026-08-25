use std::{net::SocketAddr, sync::Arc};

use tokio::sync::{RwLock, mpsc, oneshot};

use crate::server::{self, consumer, partition};



pub type ConsumerPoolRequest = (
    Arc<RwLock<server::init::server>>,
    Arc<RwLock<partition::Partition>>,
    Vec<u8>,
    SocketAddr,
);
struct ConsumerWorkerTask {
    request: ConsumerPoolRequest,
    previous_signal: Option<oneshot::Receiver<()>>,
    completion_signal: oneshot::Sender<()>,
}
pub fn ConsumerPool(worker_count: usize,) -> mpsc::Sender<ConsumerPoolRequest> {
    let (sender, mut queue) =mpsc::channel::<ConsumerPoolRequest>(1024);

    if worker_count == 0 {
        eprintln!(
            "Partition worker pool cannot have zero workers"
        );

        return sender;
    }

    

    let mut worker_senders = Vec::new();

    for _ in 0..worker_count {
        let (worker_sender,mut worker_receiver,) = mpsc::channel::<ConsumerWorkerTask>(256);

        worker_senders.push(worker_sender);

        tokio::spawn(async move {
            while let Some(task) =worker_receiver.recv().await{
                let ConsumerWorkerTask {
                    request,
                    previous_signal,
                    completion_signal,
                } = task;

                let (
                    server,
                    partition,
                    value,
                    client_addr,
                ) = request;

                // println!(
                //     "Partition worker received request - waiting for previous signal"
                // );

                // Wait for the previous partition request
                // to finish its write.
                if let Some(previous_signal) =previous_signal{
                    // println!(
                    //     "Waiting for previous partition request..."
                    // );

                    if let Err(e) =previous_signal.await{
                        eprintln!("Previous partition signal failed: {}",e);
                    }

                    // println!(
                    //     "Previous partition request signal received"
                    // );
                }

                // println!(
                //     "Partition worker starting write"
                // );


                let response_writer_signal = {
                    let server_guard =server.read().await;
                    server_guard.response_pool.clone()
                };

                let consumers={
                    let partition_guard=partition.read().await;
                    Arc::clone(&partition_guard.consumers)
                };
                
                let consumers_list = {
                let consumers_guard = consumers.read().await;
                    consumers_guard.clone()
                };

                for consumer in consumers_list {

                    if let Err(send_err) =
                        response_writer_signal
                            .send((
                                Arc::clone(&server),
                                consumer.consumer_addr,
                                true,
                                value.clone(),
                            ))
                            .await
                    {
                        eprintln!(
                            "Failed to queue consumer response to {}: {}",
                            consumer.consumer_addr,
                            send_err
                        );
                    }
                }

                // This request is now completely finished.
                // Wake the next request.
                // println!(
                //     "Sending partition completion signal"
                // );

                let _ =completion_signal.send(());
            }
        });
    }

        
    tokio::spawn(async move {
        let mut next_worker = 0;

        let mut previous_signal:
            Option<oneshot::Receiver<()>> = None;

        while let Some(request) =queue.recv().await{
        

            if worker_senders.is_empty() {
                eprintln!(
                    "No partition workers available"
                );
                break;
            }

            // Create the signal that THIS request will
            // send when its partition write is finished.
            let (
                completion_signal,
                completion_receiver,
            ) = oneshot::channel::<()>();

            let task = ConsumerWorkerTask {
                request,
                previous_signal: previous_signal.take(),
                completion_signal,
            };

            // The receiver becomes the "previous signal"
            // for the NEXT request.
            previous_signal =
                Some(completion_receiver);

            if let Err(e) =
                worker_senders[next_worker]
                    .send(task)
                    .await
            {
                eprintln!(
                    "Failed to dispatch partition request: {}",
                    e
                );
                break;
            }

            next_worker =
                (next_worker + 1)
                    % worker_senders.len();
        }
    });
    sender
}





pub type ConsumerRequest = (
    Arc<RwLock<server::init::server>>,
    Vec<u8>,
    Vec<u8>,
    Vec<u8>,
    SocketAddr,
);
pub fn ConsumerReg() -> mpsc::Sender<ConsumerRequest> {
    let (sender, mut receiver) = mpsc::channel::<ConsumerRequest>(1024);

    tokio::spawn(async move {
        while let Some((
            server,
            topic_name,
            group_name,
            startpoint,
            client_addr,
        )) = receiver.recv().await
        {
            // -------------------------------------------------
            // Get response pool
            // -------------------------------------------------

            let response_pool = {
                let server_guard = server.read().await;
                server_guard.response_pool.clone()
            };

            let (topic_map, consumer_grp, shard_count) = {
            let server_guard = server.read().await;

            let shard = server_guard.GetShard(
                &topic_name,
                server_guard.shard_count,
            );

            let topic_map = server_guard
                .shard_map
                .get(&shard)
                .ok_or_else(|| {
                    format!("Shard {} not found", shard.0)
                }).unwrap()
                .clone();

            let consumer_grp = server_guard.consumer_grp.clone();

            (
                topic_map,
                consumer_grp,
                server_guard.shard_count,
            )
        };
        // --------------------------------------------------
                // Parse start point
                // --------------------------------------------------

                let start_point = u64::from_be_bytes(
                    startpoint
                        .try_into()
                        .map_err(|_| "Expected 8 bytes").unwrap()
                ) as usize;

        // --------------------------------------------------
        // Get topic
        // --------------------------------------------------

        let topic_map_guard = topic_map.read().await;

        let topic = topic_map_guard
            .get(&topic_name)
            .ok_or_else(|| {
                format!(
                    "Topic '{}' does not exist",
                    String::from_utf8_lossy(&topic_name)
                )
            }).unwrap();

        let partition_no = topic.partition_no;

// --------------------------------------------------
// Add consumer to consumer group
// --------------------------------------------------

let mut consumer_grp_guard = consumer_grp.write().await;

// --------------------------------------------------
// Generate a NEW logical consumer ID
// --------------------------------------------------

let consumer_id = consumer_grp_guard
    .consumers
    .keys()
    .max()
    .map(|id| id + 1)
    .unwrap_or(0);

// --------------------------------------------------
// Create Consumer
// --------------------------------------------------

let consumer = server::consumer::Consumer {
    consumer_id,
    consumer_addr: client_addr,
    start_point,
    offset: 0,
    group_name: group_name.clone(),
};

// --------------------------------------------------
// Store Consumer by ID
// --------------------------------------------------

consumer_grp_guard
    .consumers
    .insert(consumer_id, consumer);

// --------------------------------------------------
// Add consumer ID to group
// --------------------------------------------------

let consumers = consumer_grp_guard
    .grp
    .entry(group_name.clone())
    .or_insert_with(Vec::new);

consumers.push(consumer_id);

// Clone group members
let group_consumers = consumers.clone();

drop(consumer_grp_guard);

// --------------------------------------------------
// Rebalance all partitions
// --------------------------------------------------

let consumer_count = group_consumers.len();

if consumer_count == 0 {
    continue;
}

let mut partition_id = 0usize;

for partition in topic.partitions.values() {

    // --------------------------------------------------
    // Select consumer for this partition
    // --------------------------------------------------

    let consumer_index =
        partition_id % consumer_count;

    let consumer_id =
        group_consumers[consumer_index];

    // --------------------------------------------------
    // Get actual Consumer from consumer registry
    // --------------------------------------------------

    let consumer = {
        let consumer_grp_guard =
            consumer_grp.read().await;

        consumer_grp_guard
            .consumers
            .get(&consumer_id)
            .cloned()
            .unwrap()
    };

    // --------------------------------------------------
    // Get partition consumers
    // --------------------------------------------------

    let consumers = {
        let partition_guard =
            partition.read().await;

        Arc::clone(&partition_guard.consumers)
    };

    let mut consumers_guard =
        consumers.write().await;

    // --------------------------------------------------
    // Remove old assignments for THIS group
    // --------------------------------------------------

    consumers_guard.retain(|consumer| {
        consumer.group_name != group_name
    });

    // --------------------------------------------------
    // Add assigned consumer
    // --------------------------------------------------

    consumers_guard.push(consumer);

    partition_id += 1;
}
        
            
            // -------------------------------------------------
            // Send ACK
            // -------------------------------------------------

            if let Err(e) = response_pool
                .send((
                    Arc::clone(&server),
                    client_addr,
                    true,
                    consumer_id.to_be_bytes().to_vec(),
                ))
                .await
            {
                eprintln!(
                    "Failed to send consumer registration response: {}",
                    e
                );
            }

            // -------------------------------------------------
            // Signal completion
            // -------------------------------------------------

        }
    });

    sender
}



