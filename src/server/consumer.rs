use std::{collections::HashMap, net::SocketAddr};



#[derive(Debug)]
#[derive(Clone)]
pub struct Consumer {
    pub consumer_id:i64,
    pub consumer_addr: SocketAddr,
    pub start_point: usize,
    pub offset: usize,
    pub group_name:Vec<u8>
}

#[derive(Debug, Clone)]
pub struct Consumergrp {
    pub grp: HashMap<Vec<u8>, Vec<i64>>,
    pub consumers: HashMap<i64, Consumer>,
}

