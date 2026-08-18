//! cvsd-export — walk a reth database READ-ONLY and stream the CVSD state
//! base straight to S3: the full current state as globally-sorted
//! fixed-size records in 256 MB parts, plus one sparse index per table.
//!
//! ## Why this exists
//!
//! The CVSD reader serves recent blocks from a RAM window, but real dapp
//! traffic reads arbitrary user state — keys the chain has not touched for
//! weeks. Measured live (2026-08-18): composite eth_call success was 4.9%
//! at a FULL 5,400-block window, bounded by construction. Zero-error
//! serving needs full current state, and the cheapest correct source is
//! the writer's own database decoded by reth's own types — this tool is a
//! cursor walk, not a format parser, so it CANNOT disagree with the node.
//!
//! ## The format (deliberately dumb)
//!
//! * `accounts.<part:05>` / `slots.<part:05>` — consecutive 256 MB slices
//!   of ONE globally key-sorted record stream. This fork keeps state ONLY
//!   in the HASHED tables (Plain* are empty — measured: HashedAccounts
//!   411.5M rows, PlainAccountState 0), so records are hashed-keyed and the
//!   reader hashes an address/slot before lookup:
//!   accounts: `keccak(addr)[32] | nonce_be[8] | balance_be[32] | code_hash[32]` (104 B)
//!   slots:    `keccak(addr)[32] | keccak(key)[32] | value_be[32]`              (96 B)
//! * `accounts.idx` / `slots.idx` — every 1024th record's key + its GLOBAL
//!   byte offset (u64 be). A reader loads the whole index into RAM at boot
//!   (~7 MB + ~64 MB) and answers any key with ONE ranged GET.
//! * `meta.json` — export block height, counts, part size. The reader
//!   inserts results as base observations AT THAT BLOCK; the segment
//!   stream carries every change after it.
//!
//! ## Streaming, no local disk
//!
//! Parts buffer in RAM and upload via `aws s3 cp -` as they fill — the
//! host keeps ZERO bytes. Run against a MOUNTED SNAPSHOT (`ro,noload`),
//! never the live database.

use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Stdio};

use clap::Parser;
use reth_db::mdbx::DatabaseArguments;
use reth_db::{open_db_read_only, tables};
use reth_db_api::cursor::DbCursorRO;
use reth_db_api::transaction::DbTx;
use reth_db_api::Database as _;

#[derive(Parser)]
struct Args {
    /// Path to the db directory (the one containing mdbx.dat).
    #[arg(long)]
    db: PathBuf,
    /// S3 destination, e.g. s3://bucket/prefix/statebase/v1
    #[arg(long)]
    s3: String,
    #[arg(long, default_value = "https://t3.storage.dev")]
    endpoint: String,
    #[arg(long, default_value_t = 268_435_456)] // 256 MB
    part_bytes: usize,
    #[arg(long, default_value_t = 1024)]
    index_every: u64,
}

struct Table {
    name: &'static str,
    s3: String,
    endpoint: String,
    part_bytes: usize,
    index_every: u64,
    buf: Vec<u8>,
    part: u32,
    global_off: u64,
    count: u64,
    idx: Vec<u8>,
}

impl Table {
    fn new(a: &Args, name: &'static str) -> Self {
        Self {
            name,
            s3: a.s3.trim_end_matches('/').to_string(),
            endpoint: a.endpoint.clone(),
            part_bytes: a.part_bytes,
            index_every: a.index_every,
            buf: Vec::with_capacity(a.part_bytes),
            part: 0,
            global_off: 0,
            count: 0,
            idx: Vec::new(),
        }
    }

    fn upload(&self, key_suffix: &str, body: &[u8]) -> eyre::Result<()> {
        let dst = format!("{}/{}", self.s3, key_suffix);
        let mut child = Command::new("aws")
            .args(["--endpoint-url", &self.endpoint, "s3", "cp", "-", &dst, "--only-show-errors"])
            .stdin(Stdio::piped())
            .spawn()?;
        child.stdin.as_mut().unwrap().write_all(body)?;
        let st = child.wait()?;
        eyre::ensure!(st.success(), "upload of {dst} failed");
        Ok(())
    }

    fn push(&mut self, rec: &[u8], key: &Vec<u8>) -> eyre::Result<()> {
        if self.count % self.index_every == 0 {
            self.idx.extend_from_slice(key);
            self.idx.extend_from_slice(&self.global_off.to_be_bytes());
        }
        self.buf.extend_from_slice(rec);
        self.global_off += rec.len() as u64;
        self.count += 1;
        if self.buf.len() >= self.part_bytes {
            let body = std::mem::take(&mut self.buf);
            self.upload(&format!("{}.{:05}", self.name, self.part), &body)?;
            eprintln!("{}.{:05} uploaded ({} records so far)", self.name, self.part, self.count);
            self.part += 1;
            self.buf = Vec::with_capacity(self.part_bytes);
        }
        Ok(())
    }

    fn finish(mut self) -> eyre::Result<(u64, u32)> {
        if !self.buf.is_empty() {
            let body = std::mem::take(&mut self.buf);
            self.upload(&format!("{}.{:05}", self.name, self.part), &body)?;
            self.part += 1;
        }
        let idx = std::mem::take(&mut self.idx);
        self.upload(&format!("{}.idx", self.name), &idx)?;
        eprintln!("{}: {} records, {} parts, idx {} bytes", self.name, self.count, self.part, idx.len());
        Ok((self.count, self.part))
    }
}

fn main() -> eyre::Result<()> {
    let args = Args::parse();
    let env = open_db_read_only(&args.db, DatabaseArguments::default())?;
    let tx = env.tx()?;

    // The height this state is true at: the Finish stage checkpoint.
    let mut stages = tx.cursor_read::<tables::StageCheckpoints>()?;
    let mut block = 0u64;
    let mut w = stages.walk(None)?;
    while let Some((name, cp)) = w.next().transpose()? {
        if name == "Finish" {
            block = cp.block_number;
        }
    }
    eyre::ensure!(block > 0, "no Finish stage checkpoint — refusing to export unanchored state");
    eprintln!("exporting state at block {block}");
    // Sanity: entry counts straight from MDBX stat, before any walking.
    eprintln!(
        "table entries: PlainAccountState={:?} PlainStorageState={:?} Bytecodes={:?} Headers={:?}",
        tx.entries::<tables::PlainAccountState>(),
        tx.entries::<tables::PlainStorageState>(),
        tx.entries::<tables::Bytecodes>(),
        tx.entries::<tables::Headers>(),
    );
    eprintln!(
        "hashed: HashedAccounts={:?} HashedStorages={:?} AccountChangeSets={:?}",
        tx.entries::<tables::HashedAccounts>(),
        tx.entries::<tables::HashedStorages>(),
        tx.entries::<tables::AccountChangeSets>(),
    );

    let mut accounts = Table::new(&args, "accounts");
    {
        let mut cur = tx.cursor_read::<tables::HashedAccounts>()?;
        let mut walker = cur.walk(None)?;
        let mut rec = [0u8; 104];
        while let Some((hashed, acct)) = walker.next().transpose()? {
            rec[..32].copy_from_slice(hashed.as_slice());
            rec[32..40].copy_from_slice(&acct.nonce.to_be_bytes());
            rec[40..72].copy_from_slice(&acct.balance.to_be_bytes::<32>());
            match acct.bytecode_hash {
                Some(h) => rec[72..104].copy_from_slice(h.as_slice()),
                None => rec[72..104].fill(0),
            }
            accounts.push(&rec, &rec[..32].to_vec())?;
        }
    }
    let (n_accounts, parts_accounts) = accounts.finish()?;

    let mut slots = Table::new(&args, "slots");
    {
        let mut cur = tx.cursor_read::<tables::HashedStorages>()?;
        let mut walker = cur.walk(None)?;
        let mut rec = [0u8; 96];
        while let Some((hashed_addr, entry)) = walker.next().transpose()? {
            rec[..32].copy_from_slice(hashed_addr.as_slice());
            rec[32..64].copy_from_slice(entry.key.as_slice());
            rec[64..96].copy_from_slice(&entry.value.to_be_bytes::<32>());
            slots.push(&rec, &rec[..64].to_vec())?;
        }
    }
    let (n_slots, parts_slots) = slots.finish()?;

    let meta = serde_json::to_vec_pretty(&serde_json::json!({
        "v": 2,
        "block": block,
        "accounts": n_accounts,
        "slots": n_slots,
        "parts": {"accounts": parts_accounts, "slots": parts_slots},
        "record": {"accounts": 104, "slots": 96},
        "keyed": "hashed",
        "part_bytes": args.part_bytes,
        "index_every": args.index_every,
    }))?;
    let t = Table::new(&args, "meta");
    t.upload("meta.json", &meta)?;
    eprintln!("done at block {block}");
    Ok(())
}
