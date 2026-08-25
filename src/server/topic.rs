use std::{collections::HashMap, error::Error, sync::Arc};

use tokio::sync::RwLock;

use crate::server::partition;



#[derive(Debug)]

pub struct TopicMap {
    map: HashMap<Vec<u8>, topic>,
}

impl TopicMap {
    pub fn new() -> TopicMap {
        Self {
            map: HashMap::new(),
        }
    }

    pub fn insert(&mut self,topic_buf: Vec<u8>,topic: topic) {
        self.map.insert(topic_buf, topic);
    }

    pub fn get(&self,topic_buf: &Vec<u8>) -> Option<&topic> {
        self.map.get(topic_buf)
    }
}
#[derive(Debug)]

pub struct topic {
    pub partition_no: usize,
    pub partitions: HashMap<usize, Arc<RwLock<partition::Partition>>>,
    pub consumer_no:usize
}

impl topic {
    pub fn new(topic_name: &Vec<u8>,partition_no: usize) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let partitions =partition::CreatePartitions(topic_name, partition_no)?;

        Ok(Self {
            partition_no,
            partitions,
            consumer_no:0
        })
    }
}