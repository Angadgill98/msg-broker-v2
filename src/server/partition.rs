use std::{collections::HashMap, error::Error, io::Write, sync::Arc};

use tokio::sync::RwLock;

use crate::server::consumer;





#[derive(Debug)]
pub struct Partition {
    id: usize,
    file_name: String,
    pub consumers: Arc<RwLock<Vec<consumer::Consumer>>> ,
}

impl Partition {
    pub fn WriteTOFile(
        &self,
        value: Vec<u8>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut file =
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.file_name)?;

        file.write_all(&value)?;
        file.write_all(b"\n")?;

        Ok(())
    }
}




pub fn CreatePartitions(topic_name: &[u8],partition_no: usize,) -> Result<HashMap<usize, Arc<RwLock<Partition>>>,Box<dyn Error + Send + Sync>,>{
    let topic_name =
        String::from_utf8(topic_name.to_vec())
            .map_err(|e| {
                format!(
                    "Invalid UTF-8 topic name: {}",
                    e
                )
            })?;

    if partition_no == 0 {
        return Err(
            "Partition count cannot be zero"
                .into()
        );
    }

    let mut partitions = HashMap::new();

    for i in 0..partition_no {
        let file_name =
            format!(
                "{}_partition_{}.log",
                topic_name,
                i
            );

        std::fs::File::create(&file_name)
            .map_err(|e| {
                format!(
                    "Failed to create partition file '{}': {}",
                    file_name,
                    e
                )
            })?;

        let partition = Partition {
            id: i,
            file_name,
            consumers: Arc::new(RwLock::new(Vec::new())),
        };

        partitions.insert(
            i,
            Arc::new(RwLock::new(partition)),
        );
    }

    Ok(partitions)
}
