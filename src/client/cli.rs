use std::error::Error;
use std::io::Write;

use tokio::io::{self, AsyncBufReadExt, BufReader};

use crate::client::init::client;

pub struct Cli;

impl Cli {
    pub async fn init(client: &mut client) -> Result<(), Box<dyn Error>> {
        dotenvy::dotenv().ok();

        println!("Commands:");
        println!("  topic <name> <partitions>");
        println!("  insert <topic> <key> <value>");
        println!("  subscribe <topic> <group_name> <start_point>");
        println!("  exit");

        Self::run(client).await?;

        Ok(())
    }

    async fn run(
        client: &mut client,
    ) -> Result<(), Box<dyn Error>> {
        let stdin = io::stdin();
        let mut reader = BufReader::new(stdin);

        loop {
            print!("> ");
            std::io::stdout().flush()?;

            let mut input = String::new();

            reader.read_line(&mut input).await?;

            let input = input.trim();

            if input.is_empty() {
                continue;
            }

            if input == "exit" {
                break;
            }

            let parts: Vec<&str> =
                input.split_whitespace().collect();

            match parts[0] {

                // =====================================================
                // TOPIC
                // =====================================================

                "topic" => {
                    if parts.len() != 3 {
                        println!(
                            "Usage: topic <name> <partitions>"
                        );
                        continue;
                    }

                    let topic =
                        parts[1].to_string();

                    let partition_no: u64 =
                        match parts[2].parse() {
                            Ok(v) => v,

                            Err(_) => {
                                println!(
                                    "Invalid partition count"
                                );
                                continue;
                            }
                        };

                    if let Err(e) = client
                        .insert_topic(
                            topic,
                            partition_no,
                        )
                        .await
                    {
                        eprintln!(
                            "Request failed: {}",
                            e
                        );
                    }
                }

                // =====================================================
                // INSERT
                // =====================================================

                "insert" => {
                    if parts.len() != 4 {
                        println!(
                            "Usage: insert <topic> <key> <value>"
                        );
                        continue;
                    }

                    let topic =
                        parts[1].to_string();

                    let key =
                        if parts[2] == "-" {
                            None
                        } else {
                            Some(parts[2].to_string())
                        };

                    let value =
                        parts[3].to_string();

                    if let Err(e) = client
                        .send_topic_data(
                            topic,
                            key,
                            value,
                        )
                        .await
                    {
                        eprintln!(
                            "Request failed: {}",
                            e
                        );
                    }
                }

                // =====================================================
                // SUBSCRIBE
                // =====================================================

                "subscribe" => {
                    if parts.len() != 4 {
                        println!(
                            "Usage: subscribe <topic> <group_name> <start_point>"
                        );
                        continue;
                    }

                    let topic = parts[1].to_string();

                    let group_name = parts[2].to_string();

                    let start_point: usize =
                        match parts[3].parse() {
                            Ok(v) => v,

                            Err(_) => {
                                println!("Invalid start point");
                                continue;
                            }
                        };

                    if let Err(e) = client
                        .subscribe(
                            topic,
                            group_name,
                            start_point,
                        )
                        .await
                    {
                        eprintln!(
                            "Subscribe failed: {}",
                            e
                        );
                    }
                }
                // =====================================================
                // UNKNOWN
                // =====================================================

                _ => {
                    println!(
                        "Unknown command: {}",
                        parts[0]
                    );
                }
            }
        }

        Ok(())
    }
}