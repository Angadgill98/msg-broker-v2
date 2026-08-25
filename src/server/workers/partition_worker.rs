use std::{net::SocketAddr, sync::Arc};

use tokio::sync::{RwLock, mpsc, oneshot};

use crate::server::{self, partition, workers::consumer_worker};



pub type PartitionPoolRequest = (
    Arc<RwLock<server::init::server>>,
    Arc<RwLock<partition::Partition>>,
    Vec<u8>,
    SocketAddr,
);
struct PartitionWorkerTask {
    request: PartitionPoolRequest,
    previous_signal: Option<oneshot::Receiver<()>>,
    completion_signal: oneshot::Sender<()>,
}
pub fn PartitionPool(worker_count: usize,) -> mpsc::Sender<PartitionPoolRequest> {
    let (sender, mut queue) =mpsc::channel::<PartitionPoolRequest>(1024);

    if worker_count == 0 {
        eprintln!(
            "Partition worker pool cannot have zero workers"
        );

        return sender;
    }

    

    let mut worker_senders = Vec::new();
    let consumer_ppol=consumer_worker::ConsumerPool(4);
    for _ in 0..worker_count {
        let (worker_sender,mut worker_receiver,) = mpsc::channel::<PartitionWorkerTask>(256);

        worker_senders.push(worker_sender);
        let consumer_pool=consumer_ppol.clone();
        tokio::spawn(async move {
            while let Some(task) =worker_receiver.recv().await{
                let PartitionWorkerTask {
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

                let partition_guard =partition.write().await;

                let response_writer_signal = {
                    let server_guard =server.read().await;
                    server_guard.response_pool.clone()
                };

                match partition_guard.WriteTOFile(value.clone()){
                    Ok(()) => {
                        // println!(
                        //     "Partition write completed"
                        // );

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
                            eprintln!(
                                "Failed to queue success response: {}",
                                e
                            );
                        }
                        if let Err(e) =
                        consumer_pool
                            .send((
                                Arc::clone(&server),
                                Arc::clone(&partition),
                                value,
                                client_addr,
                            ))
                            .await
                        {
                            eprintln!(
                                "Failed to queue consumer notification: {}",
                                e
                            );
                        }
                    }

                    Err(e) => {
                        let error_message =
                            e.to_string().into_bytes();

                        if let Err(send_err) =
                            response_writer_signal
                                .send((
                                    Arc::clone(&server),
                                    client_addr,
                                    false,
                                    error_message,
                                ))
                                .await
                        {
                            eprintln!(
                                "Failed to queue error response: {}",
                                send_err
                            );
                        }

                    }
                }

                // println!(
                //     "Worker writing to partition {} and file_name {}",
                //     partition_guard.id,
                //     partition_guard.file_name
                // );

                // This request is now completely finished.
                // Wake the next request.
                // println!(
                //     "Sending partition completion signal"
                // );

                let _ =
                    completion_signal.send(());
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

            let task = PartitionWorkerTask {
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
