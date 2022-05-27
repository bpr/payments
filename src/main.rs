use anyhow::{anyhow, Result};
use csv::StringRecord;
use rust_decimal::prelude::*;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::BufReader;

struct AccountInfo {
    available: Decimal,
    held: Decimal,
    is_locked: bool,
}

impl Default for AccountInfo {
    fn default() -> Self {
        Self {
            available: Decimal::ZERO,
            held: Decimal::ZERO,
            is_locked: false,
        }
    }
}

type ClientId = u16;
type TxnId = u32;

#[derive(Clone, Copy)]
enum Txn {
    Deposit {
        client: ClientId,
        tx: TxnId,
        amount: Decimal,
    },
    Withdrawal {
        client: ClientId,
        tx: TxnId,
        amount: Decimal,
    },
    Dispute {
        client: ClientId,
        tx: TxnId,
    },
    Resolve {
        client: ClientId,
        tx: TxnId,
    },
    Chargeback {
        client: ClientId,
        tx: TxnId,
    },
}

// deposit/withdrawal are different from dispute/resolve/chargeback
// For the former, their tx refers to themselves
#[derive(Clone, Copy)]
enum TxnKind {
    DepositKind,
    WithdrawalKind,
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() == 2 {
        process_file(&args[1])
    } else {
        Ok(())
    }
}

fn txn_of_string_record(r: &StringRecord) -> Result<Txn> {
    if r.len() == 3 {
        let client = r.get(1).unwrap().parse::<ClientId>()?;
        let tx = r.get(2).unwrap().parse::<TxnId>()?;
        match r.get(0) {
            Some("dispute") => Ok(Txn::Dispute { client, tx }),
            Some("resolve") => Ok(Txn::Resolve { client, tx }),
            Some("chargeback") => Ok(Txn::Chargeback { client, tx }),
            _ => Err(anyhow!("invalid param")),
        }
    } else if r.len() == 4 {
        let client = r.get(1).unwrap().parse::<ClientId>()?;
        let tx = r.get(2).unwrap().parse::<TxnId>()?;
        let amount = r.get(3).unwrap().parse::<Decimal>()?.round_dp(4);
        if amount > Decimal::ZERO {
            match r.get(0) {
                Some("deposit") => Ok(Txn::Deposit { client, tx, amount }),
                Some("withdrawal") => Ok(Txn::Withdrawal { client, tx, amount }),
                _ => Err(anyhow!("invalid param")),
            }
        } else {
            Err(anyhow!("update amount less than or equal to zero"))
        }
    } else {
        Err(anyhow!("invalid record"))
    }
}

fn process_file(filename: &str) -> Result<()> {
    let file = File::open(filename)?;
    let reader = BufReader::new(file);

    let mut rdr = csv::ReaderBuilder::new()
        .flexible(true)
        .comment(Some(b'#'))
        .from_reader(reader);

    let mut accounts: HashMap<ClientId, AccountInfo> = HashMap::new();
    let mut transactions: HashMap<TxnId, (TxnKind, ClientId, Decimal)> = HashMap::new();
    let mut disputed: HashSet<TxnId> = HashSet::new();

    for result in rdr.records() {
        if let Err(e) = result {
            // Skip bad data lines
            eprintln!("Bad record: {:?}", e);
            continue;
        }
        let mut record = result.unwrap();
        record.trim(); // Ensure that all fields are trimmed
        eprintln!("record: {:?}", &record);
        let txn = txn_of_string_record(&record);
        if let Err(e) = txn {
            // Skip bad data lines
            eprintln!("Bad txn: {:?}", e);
            continue;
        }
        match txn.unwrap() {
            Txn::Deposit { client, tx, amount } => {
                let acct_info = accounts.entry(client).or_insert_with(AccountInfo::default);
                if acct_info.is_locked {
                    eprintln!("Client account {:?} is locked, skipping", client);
                    continue;
                }
                // We need to keep all transactions in case they're disputed.
                // No guidance is given on how to behave differently with respect to
                // deposits vs withdrawals.
                if transactions.get(&tx).is_none() {
                    transactions.insert(tx, (TxnKind::DepositKind, client, amount));
                } else {
                    eprintln!("Duplicate txn in record {:?}", &record);
                    continue;
                }
                acct_info.available += amount;
            }
            Txn::Withdrawal { client, tx, amount } => {
                let acct_info = accounts.entry(client).or_insert_with(AccountInfo::default);
                if acct_info.is_locked {
                    eprintln!("Client account {:?} is locked, skipping", client);
                    continue;
                }
                // We need to keep all transactions in case they're disputed.
                // No guidance is given on how to behave differently with respect to
                // deposits vs withdrawals.
                if transactions.get(&tx).is_none() {
                    transactions.insert(tx, (TxnKind::WithdrawalKind, client, amount));
                } else {
                    eprintln!("Duplicate txn in record {:?}", &record);
                    continue;
                }
                if amount <= acct_info.available {
                    acct_info.available -= amount;
                } else {
                    eprintln!("Attempt to withdraw more than available {:?}", &record);
                    continue;
                }
            }
            Txn::Dispute { client, tx } => {
                let txn = transactions.get(&tx);
                if txn.is_none() {
                    // Ignore and assume it's an error on partners side
                    continue;
                }
                // The fact that _txn_kind is not used indicates a flaw. According
                // to the problem description, disputed deposits and withdrawals
                // are handled the same.
                let (_txn_kind, client_id2, amount) = txn.unwrap();
                if client != *client_id2 {
                    eprintln!(
                        "transaction {} clients don't match, {} and {}",
                        tx, client, *client_id2
                    );
                    continue;
                }
                let acct_info = accounts.entry(client).or_insert_with(AccountInfo::default);
                if acct_info.is_locked {
                    eprintln!("Client account {:?} is locked, skipping", client);
                    continue;
                }
                if acct_info.available >= *amount {
                    acct_info.available -= amount;
                    acct_info.held += amount;
                    disputed.insert(tx);
                } else {
                    eprintln!("'dispute' without enough available {:?}", &record);
                    continue;
                }
            }
            Txn::Resolve { client, tx } => {
                let txn = transactions.get(&tx);
                if txn.is_none() {
                    // Ignore and assume it's an error on partners side
                    continue;
                }
                // The fact that _txn_kind is not used indicates a flaw. According
                // to the problem description, disputed deposits and withdrawals
                // are handled the same.
                let (_txn_kind, client_id2, amount) = txn.unwrap();
                if client != *client_id2 {
                    eprintln!(
                        "transaction {} clients don't match, {} and {}",
                        tx, client, *client_id2
                    );
                    continue;
                }
                let acct_info = accounts.entry(client).or_insert_with(AccountInfo::default);
                if acct_info.is_locked {
                    eprintln!("Client account {:?} is locked, skipping", client);
                    continue;
                }
                if !disputed.contains(&tx) {
                    eprintln!("'resolve' called on undisputed transaction {}", tx);
                    continue;
                } else {
                    acct_info.available += amount;
                    acct_info.held -= amount;
                    disputed.remove(&tx);
                }
            }
            Txn::Chargeback { client, tx } => {
                let txn = transactions.get(&tx);
                if txn.is_none() {
                    // Ignore and assume it's an error on partners side
                    continue;
                }
                // The fact that _txn_kind is not used indicates a flaw. According
                // to the problem description, disputed deposits and withdrawals
                // are handled the same.
                let (_txn_kind, client_id2, amount) = txn.unwrap();
                if client != *client_id2 {
                    eprintln!(
                        "transaction {} clients don't match, {} and {}",
                        tx, client, *client_id2
                    );
                    continue;
                }
                let acct_info = accounts.entry(client).or_insert_with(AccountInfo::default);
                if acct_info.is_locked {
                    eprintln!("Client account {:?} is locked, skipping", client);
                    continue;
                }
                if !disputed.contains(&tx) {
                    eprintln!("'chargeback' called on undisputed transaction {}", tx);
                    continue;
                } else {
                    acct_info.held -= amount;
                    acct_info.is_locked = true;
                    disputed.remove(&tx);
                }
            }
        }
    }

    println!("\n");
    let (client, available, held, total, locked) =
        ("client", "available", "held", "total", "locked");

    println!("{client:16}, {available:16}, {held:16}, {total:16}, {locked:16}");
    for (client_id, acct_info) in accounts {
        // The output was justified when using strings, but not when using ints
        let client_id = client_id.to_string();
        let available = (acct_info.available).to_string();
        let held = (acct_info.held).to_string();
        let total = (acct_info.available + acct_info.held).to_string();
        let locked = acct_info.is_locked;
        println!("{client_id:16}, {available:16}, {held:16}, {total:16}, {locked:16}");
    }

    Ok(())
}
