use std::collections::HashMap;
use std::error::Error;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::watch;

use crate::consumer::inti::consumer;
use crate::producer::init::producer;





pub struct client{
    // socket:OwnedWriteHalf,
    consumer:consumer,
    producer:producer,

    request_queue_signal:Sender<Vec<u8>>,
    response_signal: watch::Receiver<Vec<u8>>,
    consumer_socket:Sender<Vec<u8>>
}




impl client {
    pub async fn init() -> Result<Self, Box<dyn Error>> {
        let socket = client::CreateSocket().await?;

        let (mut reader, writer) = socket.into_split();

        let (response_tx, response_rx) =watch::channel(Vec::<u8>::new());

        let response_tx_clone = response_tx.clone();

    
        tokio::spawn(async move {
            loop {
                // -------------------------
                // Read ACK
                // -------------------------
                let mut ack_buf = [0u8; 1];

                if let Err(e) = reader.read_exact(&mut ack_buf).await {
                    eprintln!("Server connection closed: {}", e);
                    break;
                }

                let ack = ack_buf[0] == 1;

                // -------------------------
                // Read response length
                // -------------------------
                let mut len_buf = [0u8; 8];

                if let Err(e) = reader.read_exact(&mut len_buf).await {
                    eprintln!("Failed to read response length: {}", e);
                    break;
                }

                let len = u64::from_be_bytes(len_buf) as usize;

                // -------------------------
                // Read response
                // -------------------------
                let mut response = vec![0u8; len];

                if let Err(e) = reader.read_exact(&mut response).await {
                    eprintln!("Failed to read response: {}", e);
                    break;
                }

                // -------------------------
                // Handle response
                // -------------------------

                if ack {
                    println!("ACK recived");
                    println!("Response: {:?}", response);
                    let _ =response_tx_clone.send(response);
                } else {
                    println!(
                        "Request failed: {}",
                        String::from_utf8_lossy(&response)
                    );
                }
            }
        });


        let consumer_socket: TcpStream=client::CreateSocket().await?;

        let(mut consumer_reader,consumer_writer)=consumer_socket.into_split();

        let (consumer_response_tx, consumer_response_rx) =mpsc::channel(1024);

        tokio::spawn(async move {
            loop {
                // -------------------------
                // Read ACK
                // -------------------------
                let mut ack_buf = [0u8; 1];

                if let Err(e) = consumer_reader.read_exact(&mut ack_buf).await {
                    eprintln!("Server connection closed: {}", e);
                    break;
                }

                let ack = ack_buf[0] == 1;

                // -------------------------
                // Read response length
                // -------------------------
                let mut len_buf = [0u8; 8];

                if let Err(e) = consumer_reader.read_exact(&mut len_buf).await {
                    eprintln!("Failed to read response length: {}", e);
                    break;
                }

                let len = u64::from_be_bytes(len_buf) as usize;

                // -------------------------
                // Read response
                // -------------------------
                let mut response = vec![0u8; len];

                if let Err(e) = consumer_reader.read_exact(&mut response).await {
                    eprintln!("Failed to read response: {}", e);
                    break;
                }

                // -------------------------
                // Handle response
                // -------------------------

                if ack {
                    println!("ACK recived");
                    println!("Response: {:?}", response);
                } else {
                    println!(
                        "Request failed: {}",
                        String::from_utf8_lossy(&response)
                    );
                }
            }
        });

      
        Ok(Self {
            
            consumer: consumer::new(),
            producer: producer::new(),
            request_queue_signal:client::RequestQueue(writer),
            response_signal:response_rx,
            consumer_socket:consumer_response_tx
        })
    }

    
    fn RequestQueue(mut stream: OwnedWriteHalf) -> Sender<Vec<u8>> {
        let (sender, mut receiver) = mpsc::channel::<Vec<u8>>(1024);

        tokio::spawn(async move {
            const MAX_BATCH_SIZE: usize = 64 * 1024;
            const MAX_REQUESTS: u64 = 100;
            const BATCH_TIMEOUT: std::time::Duration =
                std::time::Duration::from_millis(1);

            let mut buffer = Vec::new();
            let mut request_count: u64 = 0;

            loop {
                let request = if request_count == 0 {
                    match receiver.recv().await {
                        Some(request) => request,
                        None => break,
                    }
                } else {
                    match tokio::time::timeout(
                        BATCH_TIMEOUT,
                        receiver.recv(),
                    )
                    .await
                    {
                        Ok(Some(request)) => request,

                        Ok(None) => {
                            break;
                        }

                        Err(_) => {
                            // -----------------------------
                            // Timeout -> flush batch
                            // -----------------------------

                            let buffer_len =
                                buffer.len() as u64;

                            let mut batch =
                                Vec::with_capacity(
                                    16 + buffer.len()
                                );

                            // Number of requests
                            batch.extend_from_slice(
                                &request_count.to_be_bytes()
                            );

                            // Total buffer length
                            batch.extend_from_slice(
                                &buffer_len.to_be_bytes()
                            );

                            // All requests
                            batch.extend_from_slice(&buffer);

                            if let Err(e) =
                                stream.write_all(&batch).await
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

                // -----------------------------
                // Add request to batch
                // -----------------------------

                let request_len =
                    (request.len() as u64).to_be_bytes();

                buffer.extend_from_slice(&request_len);
                buffer.extend_from_slice(&request);

                request_count += 1;

                // -----------------------------
                // Flush if batch is full
                // -----------------------------

                if buffer.len() >= MAX_BATCH_SIZE
                    || request_count >= MAX_REQUESTS
                {
                    let buffer_len =
                        buffer.len() as u64;

                    let mut batch =
                        Vec::with_capacity(
                            16 + buffer.len()
                        );

                    // Number of requests
                    batch.extend_from_slice(
                        &request_count.to_be_bytes()
                    );

                    // Total buffer length
                    batch.extend_from_slice(
                        &buffer_len.to_be_bytes()
                    );

                    // All requests
                    batch.extend_from_slice(&buffer);

                    // Send
                    if let Err(e) =
                        stream.write_all(&batch).await
                    {
                        eprintln!(
                            "Failed to send batch: {}",
                            e
                        );
                        break;
                    }

                    // Reset
                    buffer.clear();
                    request_count = 0;
                }
            }

            // -----------------------------
            // Flush remaining requests
            // -----------------------------

            if request_count > 0 {
                let buffer_len =
                    buffer.len() as u64;

                let mut batch =
                    Vec::with_capacity(
                        16 + buffer.len()
                    );

                // Number of requests
                batch.extend_from_slice(
                    &request_count.to_be_bytes()
                );

                // Total buffer length
                batch.extend_from_slice(
                    &buffer_len.to_be_bytes()
                );

                // All requests
                batch.extend_from_slice(&buffer);

                if let Err(e) =
                    stream.write_all(&batch).await
                {
                    eprintln!(
                        "Failed to send final batch: {}",
                        e
                    );
                }
            }
        });

        sender
    }

    
    pub async fn CreateSocket() -> Result<TcpStream, Box<dyn Error>> {
        let addr = std::env::var("server_addr")
            .map_err(|_| "Environment variable 'server_addr' not defined")?;

        let stream = TcpStream::connect(addr).await?;

        Ok(stream)
    }

    pub async fn insert_topic(
        &mut self,
        topic_name: String,
        partition_no: u64,
    ) -> Result<(), Box<dyn Error>> {
        let op = "topic_insert".as_bytes();
        let op_len = (op.len() as u64).to_be_bytes();

        let topic_buf = topic_name.as_bytes();
        let topic_len = (topic_buf.len() as u64).to_be_bytes();

        let partition_buf = partition_no.to_be_bytes();

        let mut buf = Vec::new();

        buf.extend_from_slice(&op_len);
        buf.extend_from_slice(op);

        buf.extend_from_slice(&topic_len);
        buf.extend_from_slice(topic_buf);

        buf.extend_from_slice(&partition_buf);

        // Total payload length
        // let buf_len = (buf.len() as u64).to_be_bytes();

        // let mut final_buf = Vec::new();

        // final_buf.extend_from_slice(&buf_len);
        // final_buf.extend_from_slice(&buf);

        // Send to server
        self.request_queue_signal.send(buf).await?;

        let res=self.response_signal.borrow().clone();

        self.response_signal.changed().await?;

        let res = self.response_signal.borrow().clone();
        println!("res came ");
        Ok(())
    }

    pub async fn send_topic_data(
        &mut self,
        topic: String,
        key: Option<String>,
        value: String,
    ) -> Result<(), Box<dyn Error>> {
        let mut buf = Vec::new();

        let op = b"topic_data_insert";
        let op_len = (op.len() as u64).to_be_bytes();

        buf.extend_from_slice(&op_len);
        buf.extend_from_slice(op);

        let topic_buf = topic.as_bytes();
        let topic_len = (topic_buf.len() as u64).to_be_bytes();

        buf.extend_from_slice(&topic_len);
        buf.extend_from_slice(topic_buf);

        match key {
            Some(s) => {
                let key_buf = s.as_bytes();
                let key_len = (key_buf.len() as u64).to_be_bytes();

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

        // Total payload length
        // let buf_len = (buf.len() as u64).to_be_bytes();

        // let mut final_buf = Vec::with_capacity(8 + buf.len());

        // final_buf.extend_from_slice(&buf_len);
        // final_buf.extend_from_slice(&buf);

        // Send to server
        self.request_queue_signal.send(buf).await?;

        Ok(())
    }

    pub async fn subscribe(
        &mut self,
        topic: String,
        group_name: String,
        start_point: usize,
    ) -> Result<(), Box<dyn Error>> {

        let mut buf = Vec::new();

        // -------------------------------------------------
        // Operation
        // -------------------------------------------------

        let op = b"subscribe";
        let op_len = (op.len() as u64).to_be_bytes();

        buf.extend_from_slice(&op_len);
        buf.extend_from_slice(op);

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

        // -------------------------------------------------
        // Send request
        // -------------------------------------------------

        self.request_queue_signal
            .send(buf)
            .await?;

        Ok(())
    }
}

