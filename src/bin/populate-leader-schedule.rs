use std::{collections::HashMap, env};

use mysql::{prelude::Queryable, Pool};
use solana_rpc_client::nonblocking::rpc_client::RpcClient;

#[tokio::main]
async fn main() {
    dotenv::dotenv().ok();
    let rpc_client = RpcClient::new(env::var("RPC_URL").unwrap());
    let epoch = rpc_client.get_epoch_info().await.unwrap().epoch;
    let leader_schedule = rpc_client.get_leader_schedule(None).await.unwrap();
    let leader_schedule = leader_schedule.unwrap();
    let rev_leader_schedule: HashMap<u64, &String> = leader_schedule.iter().fold(HashMap::new(), |mut acc, (k, v)| {
        v.iter().for_each(|v| {
            acc.insert(*v as u64 + 432000 * epoch, &k);
        });
        acc
    });
    let mysql_url = env::var("MYSQL").unwrap();
    let pool = Pool::new(mysql_url.as_str()).unwrap();
    let mut conn = pool.get_conn().unwrap();
    // insert in batches of 1600 rows
    let stmt = "INSERT INTO leader_schedule (slot, leader) VALUES ";
    let mut query = String::from(stmt);
    let mut count = 0;
    let mut cum_count = 0;
    for (slot, leader) in rev_leader_schedule.iter() {
        query.push_str(&format!("({}, '{}'),", slot, leader));
        count += 1;
        cum_count += 1;
        if count == 1600 {
            query.pop();
            conn.exec_drop(query, ()).unwrap();
            query = String::from(stmt);
            count = 0;
            println!("inserted {}/{}", cum_count, rev_leader_schedule.len());
        }
    }
    if count > 0 {
        query.pop();
        conn.exec_drop(query, ()).unwrap();
    }
}