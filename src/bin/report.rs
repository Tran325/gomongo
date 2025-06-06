use std::{collections::HashMap, env, time};

use mysql::{prelude::Queryable, Pool};
use serde::Deserialize;
use tokio::task::JoinHandle;

const Z: f64 = 3.89059188641; // p-value 0.0001

fn p_conf_interval(n: f64, k: f64) -> (f64, f64) {
    let p = k / n;
    let a = (p + Z * Z / (2.0 * n)) / (1.0 + Z * Z / n);
    let b = Z / (1.0 + Z * Z / n) * (p * (1.0 - p) / n + Z * Z / (4.0 * n * n)).sqrt();
    (a - b, a + b)
}

fn count_conf_interval(mu: f64, stdev: f64, n: f64) -> (f64, f64) {
    let a = mu - Z * stdev / n.sqrt();
    let b = mu + Z * stdev / n.sqrt();
    (a, b)
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ValidatorInfo {
    pub identity: String,
    pub vote_pubkey: Option<String>,
    pub name: Option<String>,
}

/// Sandwicher-colluder report
/// The main metrics we're looking for here are sandwiches per slot (Sc) and proportion of slots with sandwiches (Sc_p),
/// and our hypothesis is that colluders will have a higher value in both values, compared to the cluster average.
/// Solana validators typically only receive transactions when it's close to their leader slot,
/// and colluders relays these transactions to the sandwichers, who will sandwich the transactions where feasible and submit ASAP,
/// or the tx may land on its own (without its slippage being artifically inflated!).
/// Therefore, colluders are expected to have higher Sc and Sc_p values compared to non-colluders.
/// Since txs may take a couple slots to land (sent to a colluder but landed after the colluder's leader slots), leaders
/// of prior slots (`offset_range`) will also be credited for any given sandwich. Ideally, slots farther away should receive
/// less credits, and the exact distribution should resemble that of the actual latency of sandwichable txs, but that's unimplemented for now.
#[tokio::main]
async fn main() {
    dotenv::dotenv().ok();
    let mut args = env::args();
    args.next(); // argv[0]
    let slot_range: (i64, i64) = (args.next().unwrap().parse().unwrap(), args.next().unwrap().parse().unwrap());
    let validator_info_fut: JoinHandle<Vec<ValidatorInfo>> = tokio::spawn(async move {
        let resp = reqwest::get("https://hanabi.so/api/validators/info").await.unwrap();
        let text = resp.text().await.unwrap();
        serde_json::from_str(&text).unwrap()
    });
    let now = time::Instant::now();
    let mysql_url = env::var("MYSQL").unwrap();
    let pool = Pool::new(mysql_url.as_str()).unwrap();
    let mut conn = pool.get_conn().unwrap();
    eprintln!("[+{:7}ms] Connected to MySQL", now.elapsed().as_millis());
    let offset_range = vec![0.2, 1.0, 0.6, 0.4, 0.2];
    // fetch leaders within the concerned slot range to serve as the basis of normalisation
    let leader_count = conn.exec_fold("select leader, count(*) from leader_schedule where slot between ? and ? group by leader", slot_range, HashMap::new(), |mut acc, row: (String, u64)| {
        let count = acc.entry(row.0).or_insert(0);
        *count += row.1;
        acc
    }).unwrap();
    eprintln!("[+{:7}ms] Consolidated leader schedule", now.elapsed().as_millis());
    conn.exec_drop("drop table if exists sandwich_slot", ()).unwrap();
    conn.exec_drop("create table sandwich_slot (select s.sandwich_id, min(t.slot) as slot from swap s, `transaction` t where s.tx_id=t.id group by s.sandwich_id);", ()).unwrap();
    conn.exec_drop("ALTER TABLE `sandwich_slot` CHANGE `slot` `slot` BIGINT(20) NOT NULL; ", ()).unwrap();
    conn.exec_drop("ALTER TABLE `sandwich_slot` ADD INDEX(`slot`); ", ()).unwrap();
    eprintln!("[+{:7}ms] Created temp tables", now.elapsed().as_millis());
    // mean and sd of sandwiches per slot
    let n = slot_range.1 - slot_range.0;
    let mut sx = 0.0;
    let mut sxx = 0.0;
    conn.exec_iter("SELECT count(*) FROM `sandwich_slot` where slot between ? and ? group by slot;", slot_range).unwrap().for_each(|row| {
        let count: i32 = mysql::from_row(row.unwrap());
        let x = count as f64;
        sx += x;
        sxx += x * x;
    });
    let mean = sx / n as f64;
    let stdev = (sxx / n as f64 - mean * mean).sqrt();
    eprintln!("[+{:7}ms] Consolidated frequencies", now.elapsed().as_millis());
    // raw score calculations (sandwiches in leader slot with offset to account for tx delay)
    let offset_stmt = conn.prep("select l.leader, count(*) from (SELECT slot-? as slot FROM `sandwich_slot`) t1, leader_schedule l where t1.slot=l.slot and t1.slot between ? and ? group by l.leader;").unwrap();
    let presence_offset_stmt = conn.prep("select l.leader, count(*) from (SELECT distinct slot-? as slot FROM `sandwich_slot`) t1, leader_schedule l where t1.slot=l.slot and t1.slot between ? and ? group by l.leader;").unwrap();
    let mut scores: HashMap<String, f64> = HashMap::new();
    let mut presence_scores: HashMap<String, f64> = HashMap::new();
    let mut total_score = 0.0;
    let mut total_presence_score = 0.0;
    for i in 0..offset_range.len() {
        conn.exec_iter(&offset_stmt, (i, slot_range.0, slot_range.1)).unwrap().for_each(|row| {
            let (leader, count): (String, i32) = mysql::from_row(row.unwrap());
            let count = count as f64 * offset_range[i];
            let score = scores.entry(leader).or_insert(0.0);
            *score += count;
            total_score += count;
        });
        conn.exec_iter(&presence_offset_stmt, (i, slot_range.0, slot_range.1)).unwrap().for_each(|row| {
            let (leader, count): (String, i32) = mysql::from_row(row.unwrap());
            let count = count as f64 * offset_range[i];
            let score = presence_scores.entry(leader).or_insert(0.0);
            *score += count;
            total_presence_score += count;
        });
        eprintln!("[+{:7}ms] Completed iteration {i}", now.elapsed().as_millis());
    }
    // normalise scores into an approximate measure of sandwiches per slot
    let norm_factor = offset_range.iter().sum::<f64>();
    let normalised_scores = scores.iter().map(|(k, v)| {
        let count = leader_count.get(k).unwrap_or(&0);
        (k.clone(), *v as f64 / *count as f64 / norm_factor)
    }).collect::<HashMap<String, f64>>();
    let presence_normalised_scores = presence_scores.iter().map(|(k, v)| {
        let count = leader_count.get(k).unwrap_or(&0);
        (k.clone(), *v as f64 / *count as f64 / norm_factor)
    }).collect::<HashMap<String, f64>>();
    let mut entries = normalised_scores.iter().map(|(k, v)| {
        let slots = leader_count[k] as f64;
        (k, v, presence_normalised_scores[k], v * slots, presence_normalised_scores[k] * slots, slots as i32)
    }).collect::<Vec<_>>();
    // and sort by presence, then frequency
    entries.sort_by(|a, b| {
        let a = (a.2, a.1);
        let b = (b.2, b.1);
        b.partial_cmp(&a).unwrap()
    });
    // wait for validator info
    let validator_info = validator_info_fut.await.unwrap();
    let validator_info = validator_info.into_iter().map(|v| (v.identity.clone(), v)).collect::<HashMap<String, ValidatorInfo>>();
    // print report
    println!("{},{},{},{},{},{},{},{},{},{},{},{},{},{}", "leader", "vote", "name", "Sc", "Sc_p", "R-Sc", "R-Sc_p", "slots", "Sc_p_lb", "Sc_p_ub", "Sc_p_flag", "Sc_lb", "Sc_ub", "Sc_flag");
    let w_sc_p = total_presence_score as f64 / (slot_range.1 - slot_range.0) as f64 / norm_factor;
    let w_sc = total_score as f64 / (slot_range.1 - slot_range.0) as f64 / norm_factor;
    for (leader, sc, sc_p, rsc, rsc_p, slots) in entries.iter() {
        let (lb, ub) = p_conf_interval(*slots as f64, *rsc_p);
        let (n_lb, n_ub) = count_conf_interval(mean, stdev as f64, *slots as f64);
        let entry = validator_info.get(*leader);
        let (vote, name) = match entry {
            Some(v) => (v.vote_pubkey.clone().unwrap_or("".to_string()), v.name.clone().unwrap_or("".to_string())),
            None => ("".to_string(), "".to_string())
        };
        println!("{},{},\"{}\",{},{},{},{},{},{},{},{},{},{},{}", leader, vote, name.replace("\"", "\"\""), sc, sc_p, rsc, rsc_p, slots, lb, ub, lb > w_sc_p, n_lb, n_ub, n_ub < **sc);
    }
    println!("Weighted avg Sc_p,{:.5},,,,,,,,,,,,", w_sc_p);
    println!("Weighted avg Sc,{:.5},,,,,,,,,,,,", w_sc);
    println!("Global stdev,{:.5},,,,,,,,,,,,", stdev);
    println!("Slot count,{},,,,,,,,,,,,", slot_range.1 - slot_range.0 + 1);
}
