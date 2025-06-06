use {
    backoff::{future::retry, ExponentialBackoff},
    clap::{Parser, Subcommand, ValueEnum},
    dotenv::dotenv,
    futures::{future::TryFutureExt, sink::SinkExt, stream::StreamExt},
    log::{error, info},
    solana_sdk::{pubkey::Pubkey, signature::Signature, transaction::TransactionError},
    solana_transaction_status::{EncodedTransactionWithStatusMeta, UiTransactionEncoding},
    std::{collections::HashMap, env, fmt, fs::File, str::FromStr, sync::Arc, time::Duration},
    tokio::sync::Mutex,
    yellowstone_grpc_client::{GeyserGrpcClient, GeyserGrpcClientError, Interceptor},
    yellowstone_grpc_proto::prelude::{
        subscribe_request_filter_accounts_filter::Filter as AccountsFilterDataOneof,
        subscribe_request_filter_accounts_filter_memcmp::Data as AccountsFilterMemcmpOneof,
        subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
        SubscribeRequestAccountsDataSlice, SubscribeRequestFilterAccounts,
        SubscribeRequestFilterAccountsFilter, SubscribeRequestFilterAccountsFilterMemcmp,
        SubscribeRequestFilterBlocks, SubscribeRequestFilterBlocksMeta,
        SubscribeRequestFilterEntry, SubscribeRequestFilterSlots,
        SubscribeRequestFilterTransactions, SubscribeRequestPing, SubscribeUpdateAccount,
        SubscribeUpdateTransaction, SubscribeUpdateTransactionStatus,
    },
};

type SlotsFilterMap = HashMap<String, SubscribeRequestFilterSlots>;
type AccountFilterMap = HashMap<String, SubscribeRequestFilterAccounts>;
type TransactionsFilterMap = HashMap<String, SubscribeRequestFilterTransactions>;
type TransactionsStatusFilterMap = HashMap<String, SubscribeRequestFilterTransactions>;
type EntryFilterMap = HashMap<String, SubscribeRequestFilterEntry>;
type BlocksFilterMap = HashMap<String, SubscribeRequestFilterBlocks>;
type BlocksMetaFilterMap = HashMap<String, SubscribeRequestFilterBlocksMeta>;

#[derive(Debug, Clone)]
struct Args {
    endpoint: String,
    x_token: Option<String>,
    commitment: Option<ArgsCommitment>,
    action: Action,
}

impl Args {
    fn new_from_env() -> anyhow::Result<Self> {
        // Load environment variables from .env file
        dotenv().ok();
        
        // Required environment variables
        let endpoint = env::var("ENDPOINT")
            .map_err(|_| anyhow::anyhow!("ENDPOINT environment variable not set"))?;
        
        // Optional environment variables
        let x_token = env::var("X_TOKEN").ok();
        
        // Parse commitment
        let commitment = env::var("COMMITMENT").ok().map(|c| {
            match c.as_str() {
                "Processed" => ArgsCommitment::Processed,
                "Confirmed" => ArgsCommitment::Confirmed,
                "Finalized" => ArgsCommitment::Finalized,
                _ => ArgsCommitment::Processed, // Default to Processed if invalid
            }
        });
        
        // Parse action
        let action_str = env::var("ACTION")
            .map_err(|_| anyhow::anyhow!("ACTION environment variable not set"))?;
        
        let action = match action_str.as_str() {
            "HealthCheck" => Action::HealthCheck,
            "HealthWatch" => Action::HealthWatch,
            "Ping" => {
                let count = env::var("PING_COUNT")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);
                Action::Ping { count }
            },
            "GetLatestBlockhash" => Action::GetLatestBlockhash,
            "GetBlockHeight" => Action::GetBlockHeight,
            "GetSlot" => Action::GetSlot,
            "IsBlockhashValid" => {
                let blockhash = env::var("BLOCKHASH")
                    .map_err(|_| anyhow::anyhow!("BLOCKHASH environment variable required for IsBlockhashValid action"))?;
                Action::IsBlockhashValid { blockhash }
            },
            "GetVersion" => Action::GetVersion,
            "Subscribe" => {
                // Create a new ActionSubscribe and populate from env vars
                let subscribe_args = Box::new(self::parse_subscribe_args_from_env()?);
                Action::Subscribe(subscribe_args)
            },
            _ => return Err(anyhow::anyhow!("Invalid ACTION value")),
        };
        
        Ok(Args {
            endpoint,
            x_token,
            commitment,
            action,
        })
    }

    fn get_commitment(&self) -> Option<CommitmentLevel> {
        Some(self.commitment.unwrap_or_default().into())
    }

    async fn connect(&self) -> anyhow::Result<GeyserGrpcClient<impl Interceptor>> {
        GeyserGrpcClient::build_from_shared(self.endpoint.clone())?
            .x_token(self.x_token.clone())?
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(10))
            .connect()
            .await
            .map_err(Into::into)
    }
}

fn parse_subscribe_args_from_env() -> anyhow::Result<ActionSubscribe> {
    // Helper function to parse boolean env vars
    let parse_bool = |key: &str| -> bool {
        env::var(key)
            .ok()
            .and_then(|val| val.parse::<bool>().ok())
            .unwrap_or(false)
    };
    
    // Helper function to parse comma-separated strings 
    let parse_string_list = |key: &str| -> Vec<String> {
        env::var(key)
            .ok()
            .map(|val| val.split(',').map(|s| s.trim().to_string()).collect())
            .unwrap_or_else(Vec::new)
    };
    
    Ok(ActionSubscribe {
        accounts: parse_bool("SUBSCRIBE_ACCOUNTS"),
        accounts_account: parse_string_list("ACCOUNTS_ACCOUNT"),
        accounts_account_path: env::var("ACCOUNTS_ACCOUNT_PATH").ok(),
        accounts_owner: parse_string_list("ACCOUNTS_OWNER"),
        accounts_memcmp: parse_string_list("ACCOUNTS_MEMCMP"),
        accounts_datasize: env::var("ACCOUNTS_DATASIZE").ok().and_then(|s| s.parse().ok()),
        accounts_token_account_state: parse_bool("ACCOUNTS_TOKEN_ACCOUNT_STATE"),
        accounts_data_slice: parse_string_list("ACCOUNTS_DATA_SLICE"),
        slots: parse_bool("SUBSCRIBE_SLOTS"),
        slots_filter_by_commitment: parse_bool("SLOTS_FILTER_BY_COMMITMENT"),
        transactions: parse_bool("SUBSCRIBE_TRANSACTIONS"),
        transactions_vote: env::var("TRANSACTIONS_VOTE").ok().and_then(|s| s.parse().ok()),
        transactions_failed: env::var("TRANSACTIONS_FAILED").ok().and_then(|s| s.parse().ok()),
        transactions_signature: env::var("TRANSACTIONS_SIGNATURE").ok(),
        transactions_account_include: parse_string_list("TRANSACTIONS_ACCOUNT_INCLUDE"),
        transactions_account_exclude: parse_string_list("TRANSACTIONS_ACCOUNT_EXCLUDE"),
        transactions_account_required: parse_string_list("TRANSACTIONS_ACCOUNT_REQUIRED"),
        transactions_status: parse_bool("SUBSCRIBE_TRANSACTIONS_STATUS"),
        transactions_status_vote: env::var("TRANSACTIONS_STATUS_VOTE").ok().and_then(|s| s.parse().ok()),
        transactions_status_failed: env::var("TRANSACTIONS_STATUS_FAILED").ok().and_then(|s| s.parse().ok()),
        transactions_status_signature: env::var("TRANSACTIONS_STATUS_SIGNATURE").ok(),
        transactions_status_account_include: parse_string_list("TRANSACTIONS_STATUS_ACCOUNT_INCLUDE"),
        transactions_status_account_exclude: parse_string_list("TRANSACTIONS_STATUS_ACCOUNT_EXCLUDE"),
        transactions_status_account_required: parse_string_list("TRANSACTIONS_STATUS_ACCOUNT_REQUIRED"),
        entry: parse_bool("SUBSCRIBE_ENTRY"),
        blocks: parse_bool("SUBSCRIBE_BLOCKS"),
        blocks_account_include: parse_string_list("BLOCKS_ACCOUNT_INCLUDE"),
        blocks_include_transactions: env::var("BLOCKS_INCLUDE_TRANSACTIONS").ok().and_then(|s| s.parse().ok()),
        blocks_include_accounts: env::var("BLOCKS_INCLUDE_ACCOUNTS").ok().and_then(|s| s.parse().ok()),
        blocks_include_entries: env::var("BLOCKS_INCLUDE_ENTRIES").ok().and_then(|s| s.parse().ok()),
        blocks_meta: parse_bool("SUBSCRIBE_BLOCKS_META"),
        ping: env::var("PING_COUNT").ok().and_then(|s| s.parse().ok()),
        resub: env::var("RESUB").ok().and_then(|s| s.parse().ok()),
    })
}

#[derive(Debug, Clone, Copy, Default)]
enum ArgsCommitment {
    #[default]
    Processed,
    Confirmed,
    Finalized,
}

impl From<ArgsCommitment> for CommitmentLevel {
    fn from(commitment: ArgsCommitment) -> Self {
        match commitment {
            ArgsCommitment::Processed => CommitmentLevel::Processed,
            ArgsCommitment::Confirmed => CommitmentLevel::Confirmed,
            ArgsCommitment::Finalized => CommitmentLevel::Finalized,
        }
    }
}

#[derive(Debug, Clone)]
enum Action {
    HealthCheck,
    HealthWatch,
    Subscribe(Box<ActionSubscribe>),
    Ping {
        count: i32,
    },
    GetLatestBlockhash,
    GetBlockHeight,
    GetSlot,
    IsBlockhashValid {
        blockhash: String,
    },
    GetVersion,
}

#[derive(Debug, Clone)]
struct ActionSubscribe {
    /// Subscribe on accounts updates
    accounts: bool,

    /// Filter by Account Pubkey
    accounts_account: Vec<String>,

    /// Path to a JSON array of account addresses
    accounts_account_path: Option<String>,

    /// Filter by Owner Pubkey
    accounts_owner: Vec<String>,

    /// Filter by Offset and Data, format: `offset,data in base58`
    accounts_memcmp: Vec<String>,

    /// Filter by Data size
    accounts_datasize: Option<u64>,

    /// Filter valid token accounts
    accounts_token_account_state: bool,

    /// Receive only part of updated data account, format: `offset,size`
    accounts_data_slice: Vec<String>,

    /// Subscribe on slots updates
    slots: bool,

    /// Filter slots by commitment
    slots_filter_by_commitment: bool,

    /// Subscribe on transactions updates
    transactions: bool,

    /// Filter vote transactions
    transactions_vote: Option<bool>,

    /// Filter failed transactions
    transactions_failed: Option<bool>,

    /// Filter by transaction signature
    transactions_signature: Option<String>,

    /// Filter included account in transactions
    transactions_account_include: Vec<String>,

    /// Filter excluded account in transactions
    transactions_account_exclude: Vec<String>,

    /// Filter required account in transactions
    transactions_account_required: Vec<String>,

    /// Subscribe on transactions_status updates
    transactions_status: bool,

    /// Filter vote transactions for transactions_status
    transactions_status_vote: Option<bool>,

    /// Filter failed transactions for transactions_status
    transactions_status_failed: Option<bool>,

    /// Filter by transaction signature for transactions_status
    transactions_status_signature: Option<String>,

    /// Filter included account in transactions for transactions_status
    transactions_status_account_include: Vec<String>,

    /// Filter excluded account in transactions for transactions_status
    transactions_status_account_exclude: Vec<String>,

    /// Filter required account in transactions for transactions_status
    transactions_status_account_required: Vec<String>,

    entry: bool,

    /// Subscribe on block updates
    blocks: bool,

    /// Filter included account in transactions
    blocks_account_include: Vec<String>,

    /// Include transactions to block message
    blocks_include_transactions: Option<bool>,

    /// Include accounts to block message
    blocks_include_accounts: Option<bool>,

    /// Include entries to block message
    blocks_include_entries: Option<bool>,

    /// Subscribe on block meta updates (without transactions)
    blocks_meta: bool,

    /// Send ping in subscribe request
    ping: Option<i32>,

    // Resubscribe (only to slots) after
    resub: Option<usize>,
}

impl Action {
    async fn get_subscribe_request(
        &self,
        commitment: Option<CommitmentLevel>,
    ) -> anyhow::Result<Option<(SubscribeRequest, usize)>> {
        Ok(match self {
            Self::Subscribe(args) => {
                let mut accounts: AccountFilterMap = HashMap::new();
                if args.accounts {
                    let mut accounts_account = args.accounts_account.clone();
                    if let Some(path) = args.accounts_account_path.clone() {
                        let accounts = tokio::task::block_in_place(move || {
                            let file = File::open(path)?;
                            Ok::<Vec<String>, anyhow::Error>(serde_json::from_reader(file)?)
                        })?;
                        accounts_account.extend(accounts);
                    }

                    let mut filters = vec![];
                    for filter in args.accounts_memcmp.iter() {
                        match filter.split_once(',') {
                            Some((offset, data)) => {
                                filters.push(SubscribeRequestFilterAccountsFilter {
                                    filter: Some(AccountsFilterDataOneof::Memcmp(
                                        SubscribeRequestFilterAccountsFilterMemcmp {
                                            offset: offset
                                                .parse()
                                                .map_err(|_| anyhow::anyhow!("invalid offset"))?,
                                            data: Some(AccountsFilterMemcmpOneof::Base58(
                                                data.trim().to_string(),
                                            )),
                                        },
                                    )),
                                });
                            }
                            _ => anyhow::bail!("invalid memcmp"),
                        }
                    }
                    if let Some(datasize) = args.accounts_datasize {
                        filters.push(SubscribeRequestFilterAccountsFilter {
                            filter: Some(AccountsFilterDataOneof::Datasize(datasize)),
                        });
                    }
                    if args.accounts_token_account_state {
                        filters.push(SubscribeRequestFilterAccountsFilter {
                            filter: Some(AccountsFilterDataOneof::TokenAccountState(true)),
                        });
                    }

                    accounts.insert(
                        "client".to_owned(),
                        SubscribeRequestFilterAccounts {
                            account: accounts_account,
                            owner: args.accounts_owner.clone(),
                            filters,
                        },
                    );
                }

                let mut slots: SlotsFilterMap = HashMap::new();
                if args.slots {
                    slots.insert(
                        "client".to_owned(),
                        SubscribeRequestFilterSlots {
                            filter_by_commitment: Some(args.slots_filter_by_commitment),
                        },
                    );
                }

                let mut transactions: TransactionsFilterMap = HashMap::new();
                if args.transactions {
                    transactions.insert(
                        "client".to_string(),
                        SubscribeRequestFilterTransactions {
                            vote: args.transactions_vote,
                            failed: args.transactions_failed,
                            signature: args.transactions_signature.clone(),
                            account_include: args.transactions_account_include.clone(),
                            account_exclude: args.transactions_account_exclude.clone(),
                            account_required: args.transactions_account_required.clone(),
                        },
                    );
                }

                let mut transactions_status: TransactionsStatusFilterMap = HashMap::new();
                if args.transactions_status {
                    transactions_status.insert(
                        "client".to_string(),
                        SubscribeRequestFilterTransactions {
                            vote: args.transactions_status_vote,
                            failed: args.transactions_status_failed,
                            signature: args.transactions_status_signature.clone(),
                            account_include: args.transactions_status_account_include.clone(),
                            account_exclude: args.transactions_status_account_exclude.clone(),
                            account_required: args.transactions_status_account_required.clone(),
                        },
                    );
                }

                let mut entry: EntryFilterMap = HashMap::new();
                if args.entry {
                    entry.insert("client".to_owned(), SubscribeRequestFilterEntry {});
                }

                let mut blocks: BlocksFilterMap = HashMap::new();
                if args.blocks {
                    blocks.insert(
                        "client".to_owned(),
                        SubscribeRequestFilterBlocks {
                            account_include: args.blocks_account_include.clone(),
                            include_transactions: args.blocks_include_transactions,
                            include_accounts: args.blocks_include_accounts,
                            include_entries: args.blocks_include_entries,
                        },
                    );
                }

                let mut blocks_meta: BlocksMetaFilterMap = HashMap::new();
                if args.blocks_meta {
                    blocks_meta.insert("client".to_owned(), SubscribeRequestFilterBlocksMeta {});
                }

                let mut accounts_data_slice = Vec::new();
                for data_slice in args.accounts_data_slice.iter() {
                    match data_slice.split_once(',') {
                        Some((offset, length)) => match (offset.parse(), length.parse()) {
                            (Ok(offset), Ok(length)) => {
                                accounts_data_slice
                                    .push(SubscribeRequestAccountsDataSlice { offset, length });
                            }
                            _ => anyhow::bail!("invalid data_slice"),
                        },
                        _ => anyhow::bail!("invalid data_slice"),
                    }
                }

                let ping = args.ping.map(|id| SubscribeRequestPing { id });

                Some((
                    SubscribeRequest {
                        slots,
                        accounts,
                        transactions,
                        transactions_status,
                        entry,
                        blocks,
                        blocks_meta,
                        commitment: commitment.map(|x| x as i32),
                        accounts_data_slice,
                        ping,
                    },
                    args.resub.unwrap_or(0),
                ))
            }
            _ => None,
        })
    }
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct AccountPretty {
    is_startup: bool,
    slot: u64,
    pubkey: Pubkey,
    lamports: u64,
    owner: Pubkey,
    executable: bool,
    rent_epoch: u64,
    data: String,
    write_version: u64,
    txn_signature: String,
}

impl From<SubscribeUpdateAccount> for AccountPretty {
    fn from(
        SubscribeUpdateAccount {
            is_startup,
            slot,
            account,
        }: SubscribeUpdateAccount,
    ) -> Self {
        let account = account.expect("should be defined");
        Self {
            is_startup,
            slot,
            pubkey: Pubkey::try_from(account.pubkey).expect("valid pubkey"),
            lamports: account.lamports,
            owner: Pubkey::try_from(account.owner).expect("valid pubkey"),
            executable: account.executable,
            rent_epoch: account.rent_epoch,
            data: hex::encode(account.data),
            write_version: account.write_version,
            txn_signature: bs58::encode(account.txn_signature.unwrap_or_default()).into_string(),
        }
    }
}

#[allow(dead_code)]
pub struct TransactionPretty {
    slot: u64,
    signature: Signature,
    is_vote: bool,
    tx: EncodedTransactionWithStatusMeta,
}

impl fmt::Debug for TransactionPretty {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        struct TxWrap<'a>(&'a EncodedTransactionWithStatusMeta);
        impl<'a> fmt::Debug for TxWrap<'a> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                let serialized = serde_json::to_string(self.0).expect("failed to serialize");
                fmt::Display::fmt(&serialized, f)
            }
        }

        f.debug_struct("TransactionPretty")
            .field("slot", &self.slot)
            .field("signature", &self.signature)
            .field("is_vote", &self.is_vote)
            .field("tx", &TxWrap(&self.tx))
            .finish()
    }
}

impl From<SubscribeUpdateTransaction> for TransactionPretty {
    fn from(SubscribeUpdateTransaction { transaction, slot }: SubscribeUpdateTransaction) -> Self {
        let tx = transaction.expect("should be defined");
        Self {
            slot,
            signature: Signature::try_from(tx.signature.as_slice()).expect("valid signature"),
            is_vote: tx.is_vote,
            tx: yellowstone_grpc_proto::convert_from::create_tx_with_meta(tx)
                .expect("valid tx with meta")
                .encode(UiTransactionEncoding::Base64, Some(u8::MAX), true)
                .expect("failed to encode"),
        }
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct TransactionStatusPretty {
    slot: u64,
    signature: Signature,
    is_vote: bool,
    index: u64,
    err: Option<TransactionError>,
}

impl From<SubscribeUpdateTransactionStatus> for TransactionStatusPretty {
    fn from(status: SubscribeUpdateTransactionStatus) -> Self {
        Self {
            slot: status.slot,
            signature: Signature::try_from(status.signature.as_slice()).expect("valid signature"),
            is_vote: status.is_vote,
            index: status.index,
            err: yellowstone_grpc_proto::convert_from::create_tx_error(status.err.as_ref())
                .expect("valid tx err"),
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env::set_var(
        env_logger::DEFAULT_FILTER_ENV,
        env::var_os(env_logger::DEFAULT_FILTER_ENV).unwrap_or_else(|| "info".into()),
    );
    env_logger::init();

    let args = Args::new_from_env()?;
    let zero_attempts = Arc::new(Mutex::new(true));

    // The default exponential backoff strategy intervals:
    // [500ms, 750ms, 1.125s, 1.6875s, 2.53125s, 3.796875s, 5.6953125s,
    // 8.5s, 12.8s, 19.2s, 28.8s, 43.2s, 64.8s, 97s, ... ]
    retry(ExponentialBackoff::default(), move || {
        let args = args.clone();
        let zero_attempts = Arc::clone(&zero_attempts);

        async move {
            let mut zero_attempts = zero_attempts.lock().await;
            if *zero_attempts {
                *zero_attempts = false;
            } else {
                info!("Retry to connect to the server");
            }
            drop(zero_attempts);

            let commitment = args.get_commitment();
            let mut client = args.connect().await.map_err(backoff::Error::transient)?;
            info!("Connected");

            match &args.action {
                Action::HealthCheck => client
                    .health_check()
                    .await
                    .map_err(anyhow::Error::new)
                    .map(|response| info!("response: {response:?}")),
                Action::HealthWatch => geyser_health_watch(client).await,
                Action::Subscribe(_) => {
                    let (request, resub) = args
                        .action
                        .get_subscribe_request(commitment)
                        .await
                        .map_err(backoff::Error::Permanent)?
                        .expect("expect subscribe action");

                    geyser_subscribe(client, request, resub).await
                }
                Action::Ping { count } => client
                    .ping(*count)
                    .await
                    .map_err(anyhow::Error::new)
                    .map(|response| info!("response: {response:?}")),
                Action::GetLatestBlockhash => client
                    .get_latest_blockhash(commitment)
                    .await
                    .map_err(anyhow::Error::new)
                    .map(|response| info!("response: {response:?}")),
                Action::GetBlockHeight => client
                    .get_block_height(commitment)
                    .await
                    .map_err(anyhow::Error::new)
                    .map(|response| info!("response: {response:?}")),
                Action::GetSlot => client
                    .get_slot(commitment)
                    .await
                    .map_err(anyhow::Error::new)
                    .map(|response| info!("response: {response:?}")),
                Action::IsBlockhashValid { blockhash } => client
                    .is_blockhash_valid(blockhash.clone(), commitment)
                    .await
                    .map_err(anyhow::Error::new)
                    .map(|response| info!("response: {response:?}")),
                Action::GetVersion => client
                    .get_version()
                    .await
                    .map_err(anyhow::Error::new)
                    .map(|response| info!("response: {response:?}")),
            }
            .map_err(backoff::Error::transient)?;

            Ok::<(), backoff::Error<anyhow::Error>>(())
        }
        .inspect_err(|error| error!("failed to connect: {error}"))
    })
    .await
    .map_err(Into::into)
}

async fn geyser_health_watch(mut client: GeyserGrpcClient<impl Interceptor>) -> anyhow::Result<()> {
    let mut stream = client.health_watch().await?;
    info!("stream opened");
    while let Some(message) = stream.next().await {
        info!("new message: {message:?}");
    }
    info!("stream closed");
    Ok(())
}

async fn geyser_subscribe(
    mut client: GeyserGrpcClient<impl Interceptor>,
    request: SubscribeRequest,
    resub: usize,
) -> anyhow::Result<()> {
    let (mut subscribe_tx, mut stream) = client.subscribe_with_request(Some(request)).await?;

    info!("stream opened");
    let mut counter = 0;
    while let Some(message) = stream.next().await {
        match message {
            Ok(msg) => {
                match msg.update_oneof {
                    Some(UpdateOneof::Account(account)) => {
                        let account: AccountPretty = account.into();
                        info!(
                            "new account update: filters {:?}, account: {:#?}",
                            msg.filters, account
                        );
                        continue;
                    }
                    Some(UpdateOneof::Transaction(tx)) => {
                        let tx: TransactionPretty = tx.into();
                        info!(
                            "new transaction update: filters {:?}, transaction: {:#?}",
                            msg.filters, tx
                        );
                        continue;
                    }
                    Some(UpdateOneof::TransactionStatus(status)) => {
                        let status: TransactionStatusPretty = status.into();
                        info!(
                            "new transaction update: filters {:?}, transaction status: {:?}",
                            msg.filters, status
                        );
                        continue;
                    }
                    Some(UpdateOneof::Ping(_)) => {
                        // This is necessary to keep load balancers that expect client pings alive. If your load balancer doesn't
                        // require periodic client pings then this is unnecessary
                        subscribe_tx
                            .send(SubscribeRequest {
                                ping: Some(SubscribeRequestPing { id: 1 }),
                                ..Default::default()
                            })
                            .await?;
                    }
                    _ => {}
                }
                info!("new message: {msg:?}")
            }
            Err(error) => {
                error!("error: {error:?}");
                break;
            }
        }

        // Example to illustrate how to resubscribe/update the subscription
        counter += 1;
        if counter == resub {
            let mut new_slots: SlotsFilterMap = HashMap::new();
            new_slots.insert("client".to_owned(), SubscribeRequestFilterSlots::default());

            subscribe_tx
                .send(SubscribeRequest {
                    slots: new_slots.clone(),
                    accounts: HashMap::default(),
                    transactions: HashMap::default(),
                    transactions_status: HashMap::default(),
                    entry: HashMap::default(),
                    blocks: HashMap::default(),
                    blocks_meta: HashMap::default(),
                    commitment: None,
                    accounts_data_slice: Vec::default(),
                    ping: None,
                })
                .await
                .map_err(GeyserGrpcClientError::SubscribeSendError)?;
        }
    }
    info!("stream closed");
    Ok(())
}
