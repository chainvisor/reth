//! cvsd-export — walk a reth database READ-ONLY and stream the CVSD state
//! base straight to S3: the full current state as globally-sorted records
//! in 256 MB parts, plus one sparse index per table.
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
//! * `codes.<part:05>` — the third tier: EVERY deployed contract's bytecode
//!   as variable records `code_hash[32] | len_be[4] | original_bytes`,
//!   sorted by hash. The emitter's content-addressed blobs cover only
//!   TOUCHED contracts; a cold contract's code has to come from here. Each
//!   record is keccak-verified against its key before upload.
//! * `<table>.idx` — sparse index: every `index_every`-th record's key +
//!   its GLOBAL byte offset (u64 be). A reader loads the indexes into RAM
//!   at boot and answers any key with ONE ranged GET. Codes index every
//!   8th record (variable sizes; a stripe is bounded by the NEXT entry's
//!   offset, so sparser would inflate the per-miss fetch).
//! * `meta.json` — export block height, counts, part size. Written LAST:
//!   it is the commit point. The reader inserts results as base
//!   observations AT THAT BLOCK; the segment stream carries every change
//!   after it.
//!
//! ## Per-part transactions + resume (why the loops look like this)
//!
//! The first run died at part 6: reth's default `DatabaseArguments` aborts
//! any read transaction older than ~5 minutes, and one transaction cannot
//! span a multi-hour walk. So every part opens a FRESH read tx and seeks
//! back to the last emitted key — which also makes the export resumable
//! across crashes for free. After each part uploads, a progress marker
//! (`progress/<table>.json`) and that part's index fragment
//! (`idxfrag/<table>.<part:05>`) are written; a restart reads the marker,
//! verifies the database still sits at the SAME block (a swapped snapshot
//! would silently mix two states into one "sorted" base — hard fail), and
//! continues from the recorded key. Finish concatenates the fragments
//! into `<table>.idx`. Every object write is idempotent: re-uploading a
//! part from the same frozen snapshot produces identical bytes.
//!
//! ## Streaming, no local disk
//!
//! Parts buffer in RAM and upload via `aws s3 cp -` as they fill — the
//! host keeps ZERO bytes. Run against a MOUNTED SNAPSHOT (`ro,noload`),
//! never the live database.

use std::io::{Read, Write};
use std::path::PathBuf;
use std::process::{Command, Stdio};

use alloy_primitives::{keccak256, B256};
use clap::Parser;
use reth_db::mdbx::{DatabaseArguments, MaxReadTransactionDuration};
use reth_db::{open_db_read_only, tables};
use reth_db_api::cursor::{DbCursorRO, DbDupCursorRO};
use reth_db_api::transaction::DbTx;
use reth_db_api::Database as _;

#[derive(Parser)]
struct Args {
    /// Path to the db directory (the one containing mdbx.dat).
    #[arg(long)]
    db: PathBuf,
    /// S3 destination, e.g. s3://bucket/prefix/statebase/h1
    #[arg(long)]
    s3: String,
    #[arg(long, default_value = "https://t3.storage.dev")]
    endpoint: String,
    #[arg(long, default_value_t = 268_435_456)] // 256 MB
    part_bytes: usize,
    #[arg(long, default_value_t = 1024)]
    index_every: u64,
    /// Codes are variable-size; a denser index keeps a cold-code stripe to
    /// a few records instead of a few megabytes.
    #[arg(long, default_value_t = 8)]
    codes_index_every: u64,
    /// Export ONLY this table (accounts|slots|codes). The three tables
    /// are independent — separate progress markers, separate parts — so
    /// three processes against the same frozen image parallelize the
    /// walk cleanly. Finish with `--finalize`.
    #[arg(long)]
    only: Option<String>,
    /// Assemble meta.json from the three tables' done-markers (run after
    /// the per-table processes complete). Verifies all three finished at
    /// the SAME block.
    #[arg(long, default_value_t = false)]
    finalize: bool,
}

// ---------------------------------------------------------------- S3 I/O

#[derive(Clone)]
struct S3 {
    base: String,
    endpoint: String,
}

impl S3 {
    fn put(&self, key: &str, body: &[u8]) -> eyre::Result<()> {
        let dst = format!("{}/{}", self.base, key);
        let mut child = Command::new("aws")
            .args(["--endpoint-url", &self.endpoint, "s3", "cp", "-", &dst, "--only-show-errors"])
            .stdin(Stdio::piped())
            .spawn()?;
        child.stdin.as_mut().unwrap().write_all(body)?;
        let st = child.wait()?;
        eyre::ensure!(st.success(), "upload of {dst} failed");
        Ok(())
    }

    /// `None` covers both "missing" and "unreachable": a lost marker only
    /// restarts a table from zero, which is correct (just slower), so the
    /// two cases do not need distinguishing.
    fn get(&self, key: &str) -> Option<Vec<u8>> {
        let dst = format!("{}/{}", self.base, key);
        let mut child = Command::new("aws")
            .args(["--endpoint-url", &self.endpoint, "s3", "cp", &dst, "-", "--only-show-errors"])
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .ok()?;
        let mut out = Vec::new();
        child.stdout.take()?.read_to_end(&mut out).ok()?;
        // Exit status is the existence signal: an empty object and a
        // missing one both read as zero bytes, but only the former exits 0.
        child.wait().ok().filter(|st| st.success())?;
        Some(out)
    }
}

// ------------------------------------------------------------- part sink

struct Sink {
    name: &'static str,
    s3: S3,
    part_bytes: usize,
    index_every: u64,
    buf: Vec<u8>,
    idx_frag: Vec<u8>,
    part: u32,
    count: u64,
    global_off: u64,
    /// The last key pushed, hex — persisted in the marker so a fresh
    /// transaction can seek straight back to it.
    last_key: Option<Vec<u8>>,
    /// In-flight part uploads: (join handle, idx fragment, marker body).
    /// The fragment and marker for part N upload only AFTER part N's
    /// body lands — resume correctness depends on that order — but the
    /// WALK continues while bodies ship. Bounded at 2 (~512MB).
    pending: std::collections::VecDeque<(std::thread::JoinHandle<eyre::Result<()>>, Vec<u8>, Vec<u8>)>,
}

impl Sink {
    /// Open the sink, resuming from the S3 progress marker if one exists.
    /// `block` pins the snapshot: a marker written against a different
    /// database height means the source changed mid-export — refuse.
    fn open(s3: &S3, name: &'static str, part_bytes: usize, index_every: u64, block: u64) -> eyre::Result<(Self, bool)> {
        let mut sink = Self {
            name,
            s3: s3.clone(),
            part_bytes,
            index_every,
            buf: Vec::with_capacity(part_bytes),
            idx_frag: Vec::new(),
            part: 0,
            count: 0,
            global_off: 0,
            last_key: None,
            pending: std::collections::VecDeque::new(),
        };
        let mut done = false;
        if let Some(body) = s3.get(&format!("progress/{name}.json")) {
            if !body.is_empty() {
                let m: serde_json::Value = serde_json::from_slice(&body)?;
                let mblock = m["block"].as_u64().unwrap_or(0);
                eyre::ensure!(
                    mblock == block,
                    "{name}: progress marker is for block {mblock} but the database is at {block} — \
                     the source snapshot changed mid-export; delete {}/progress/ to start over",
                    s3.base
                );
                sink.part = m["part"].as_u64().unwrap_or(0) as u32;
                sink.count = m["count"].as_u64().unwrap_or(0);
                sink.global_off = m["global_off"].as_u64().unwrap_or(0);
                done = m["done"].as_bool().unwrap_or(false);
                if let Some(k) = m["last_key"].as_str() {
                    if !k.is_empty() {
                        sink.last_key = Some(hex_decode(k)?);
                    }
                }
                eprintln!(
                    "{name}: resuming at part {} ({} records already exported{})",
                    sink.part, sink.count, if done { ", table complete" } else { "" }
                );
            }
        }
        Ok((sink, done))
    }

    fn push(&mut self, rec: &[u8], key: &[u8]) {
        if self.count % self.index_every == 0 {
            self.idx_frag.extend_from_slice(key);
            self.idx_frag.extend_from_slice(&self.global_off.to_be_bytes());
        }
        self.buf.extend_from_slice(rec);
        self.global_off += rec.len() as u64;
        self.count += 1;
        self.last_key = Some(key.to_vec());
    }

    fn part_full(&self) -> bool {
        self.buf.len() >= self.part_bytes
    }

    /// Drain in-flight uploads down to `max_left`, completing each
    /// part's fragment + marker in order as its body lands.
    fn reap(&mut self, max_left: usize) -> eyre::Result<()> {
        while self.pending.len() > max_left {
            let (h, frag, marker) = self.pending.pop_front().unwrap();
            h.join().map_err(|_| eyre::eyre!("upload thread panicked"))??;
            let done_part = self.part - self.pending.len() as u32 - 1;
            self.s3.put(&format!("idxfrag/{}.{:05}", self.name, done_part), &frag)?;
            self.s3.put(&format!("progress/{}.json", self.name), &marker)?;
        }
        Ok(())
    }

    /// Ship the buffered part in the BACKGROUND — the walk continues
    /// while the 256MB body uploads — then queue its index fragment and
    /// marker to land strictly after it (resume correctness). A crash
    /// between writes re-does at most the un-markered parts, same bytes.
    fn flush(&mut self, block: u64, done: bool) -> eyre::Result<()> {
        if !self.buf.is_empty() {
            let body = std::mem::take(&mut self.buf);
            let frag = std::mem::take(&mut self.idx_frag);
            let key = format!("{}.{:05}", self.name, self.part);
            let s3 = self.s3.clone();
            let count = self.count;
            let name = self.name;
            let part = self.part;
            let h = std::thread::spawn(move || {
                let r = s3.put(&key, &body);
                if r.is_ok() {
                    eprintln!("{name}.{part:05} uploaded ({count} records so far)");
                }
                r
            });
            let marker = serde_json::json!({
                "block": block,
                "part": self.part + 1,
                "count": self.count,
                "global_off": self.global_off,
                "last_key": self.last_key.as_deref().map(hex_encode).unwrap_or_default(),
                "done": false,
            });
            self.pending.push_back((h, frag, serde_json::to_vec(&marker)?));
            self.part += 1;
            self.buf = Vec::with_capacity(self.part_bytes);
            self.reap(1)?;
        }
        if done {
            self.reap(0)?;
            let marker = serde_json::json!({
                "block": block,
                "part": self.part,
                "count": self.count,
                "global_off": self.global_off,
                "last_key": self.last_key.as_deref().map(hex_encode).unwrap_or_default(),
                "done": true,
            });
            self.s3.put(&format!("progress/{}.json", self.name), &serde_json::to_vec(&marker)?)?;
        }
        Ok(())
    }

    /// Final flush + assemble `<name>.idx` from the per-part fragments.
    fn finish(mut self, block: u64) -> eyre::Result<(u64, u32)> {
        self.flush(block, true)?;
        let mut idx = Vec::new();
        for p in 0..self.part {
            let frag = self
                .s3
                .get(&format!("idxfrag/{}.{:05}", self.name, p))
                .ok_or_else(|| eyre::eyre!("{}: index fragment {p} missing — cannot assemble index", self.name))?;
            idx.extend_from_slice(&frag);
        }
        self.s3.put(&format!("{}.idx", self.name), &idx)?;
        eprintln!("{}: {} records, {} parts, idx {} bytes", self.name, self.count, self.part, idx.len());
        Ok((self.count, self.part))
    }
}

/// Assemble meta.json from the three tables' DONE markers. The markers
/// are block-pinned, so this also proves all three walked the same
/// frozen image. meta.json stays the single commit point.
fn finalize(s3: &S3, block: u64, args: &Args) -> eyre::Result<()> {
    let mut vals = std::collections::HashMap::new();
    for t in ["accounts", "slots", "codes"] {
        let body = s3
            .get(&format!("progress/{t}.json"))
            .ok_or_else(|| eyre::eyre!("{t}: no progress marker — table not exported"))?;
        let m: serde_json::Value = serde_json::from_slice(&body)?;
        eyre::ensure!(m["done"].as_bool() == Some(true), "{t}: not done");
        eyre::ensure!(
            m["block"].as_u64() == Some(block),
            "{t}: marker block {:?} != db block {block}",
            m["block"]
        );
        vals.insert(t, (m["count"].as_u64().unwrap_or(0), m["part"].as_u64().unwrap_or(0)));
    }
    let meta = serde_json::to_vec_pretty(&serde_json::json!({
        "v": 2,
        "block": block,
        "accounts": vals["accounts"].0,
        "slots": vals["slots"].0,
        "codes": vals["codes"].0,
        "parts": {"accounts": vals["accounts"].1, "slots": vals["slots"].1, "codes": vals["codes"].1},
        "record": {"accounts": 104, "slots": 96},
        "keyed": "hashed",
        "part_bytes": args.part_bytes,
        "index_every": args.index_every,
        "codes_index_every": args.codes_index_every,
    }))?;
    s3.put("meta.json", &meta)?;
    eprintln!("done at block {block}");
    Ok(())
}

fn hex_encode(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn hex_decode(s: &str) -> eyre::Result<Vec<u8>> {
    eyre::ensure!(s.len() % 2 == 0, "odd hex");
    (0..s.len() / 2).map(|i| Ok(u8::from_str_radix(&s[i * 2..i * 2 + 2], 16)?)).collect()
}

// ------------------------------------------------------------------ main

fn main() -> eyre::Result<()> {
    let args = Args::parse();
    // Belt AND suspenders: parts each use a short fresh transaction, but a
    // stalled disk must still never trip the 5-minute default abort.
    let db_args = DatabaseArguments::default()
        .with_max_read_transaction_duration(Some(MaxReadTransactionDuration::Unbounded));
    let env = open_db_read_only(&args.db, db_args)?;
    let s3 = S3 { base: args.s3.trim_end_matches('/').to_string(), endpoint: args.endpoint.clone() };

    // The height this state is true at: the Finish stage checkpoint.
    let block = {
        let tx = env.tx()?;
        let mut stages = tx.cursor_read::<tables::StageCheckpoints>()?;
        let mut block = 0u64;
        let mut w = stages.walk(None)?;
        while let Some((name, cp)) = w.next().transpose()? {
            if name == "Finish" {
                block = cp.block_number;
            }
        }
        eprintln!(
            "table entries: HashedAccounts={:?} HashedStorages={:?} Bytecodes={:?}",
            tx.entries::<tables::HashedAccounts>(),
            tx.entries::<tables::HashedStorages>(),
            tx.entries::<tables::Bytecodes>(),
        );
        block
    };
    eyre::ensure!(block > 0, "no Finish stage checkpoint — refusing to export unanchored state");
    eprintln!("exporting state at block {block}");

    if args.finalize {
        return finalize(&s3, block, &args);
    }
    let want = |t: &str| args.only.as_deref().is_none_or(|o| o == t);
    if let Some(o) = args.only.as_deref() {
        eyre::ensure!(
            matches!(o, "accounts" | "slots" | "codes"),
            "--only must be accounts|slots|codes"
        );
        eprintln!("single-table mode: {o}");
    }

    // ---- accounts: fixed 104 B records over HashedAccounts -------------
    let (mut sink, done) = Sink::open(&s3, "accounts", args.part_bytes, args.index_every, block)?;
    if want("accounts") && !done {
        loop {
            let tx = env.tx()?;
            let mut cur = tx.cursor_read::<tables::HashedAccounts>()?;
            let mut pair = match &sink.last_key {
                None => cur.first()?,
                Some(k) => {
                    let kk = B256::from_slice(k);
                    match cur.seek(kk)? {
                        Some((fk, _)) if fk == kk => cur.next()?,
                        other => other,
                    }
                }
            };
            let mut rec = [0u8; 104];
            while let Some((hashed, acct)) = pair {
                rec[..32].copy_from_slice(hashed.as_slice());
                rec[32..40].copy_from_slice(&acct.nonce.to_be_bytes());
                rec[40..72].copy_from_slice(&acct.balance.to_be_bytes::<32>());
                match acct.bytecode_hash {
                    Some(h) => rec[72..104].copy_from_slice(h.as_slice()),
                    None => rec[72..104].fill(0),
                }
                sink.push(&rec, hashed.as_slice());
                if sink.part_full() {
                    break;
                }
                pair = cur.next()?;
            }
            let at_end = !sink.part_full();
            drop(cur);
            drop(tx);
            if at_end {
                break;
            }
            sink.flush(block, false)?;
        }
    }
    let (n_accounts, parts_accounts) = if want("accounts") { sink.finish(block)? } else { (0, 0) };

    // ---- slots: fixed 96 B records over HashedStorages (dupsort) -------
    let (mut sink, done) = Sink::open(&s3, "slots", args.part_bytes, args.index_every, block)?;
    if want("slots") && !done {
        loop {
            let tx = env.tx()?;
            let mut cur = tx.cursor_read::<tables::HashedStorages>()?;
            let mut pair = match &sink.last_key {
                None => cur.first()?,
                Some(k) => {
                    let (a, s) = (B256::from_slice(&k[..32]), B256::from_slice(&k[32..]));
                    match cur.seek_by_key_subkey(a, s)? {
                        // Exact hit: continue after it. MDBX_NEXT walks
                        // across keys, so this covers the rest of the table.
                        Some(e) if e.key == s => cur.next()?,
                        // Landed on a later dup of the same key: unemitted.
                        Some(e) => Some((a, e)),
                        // `s` was the last dup of `a`: hop to the next key.
                        None => {
                            let _ = cur.seek(a)?;
                            cur.next_no_dup()?
                        }
                    }
                }
            };
            let mut rec = [0u8; 96];
            while let Some((hashed_addr, entry)) = pair {
                rec[..32].copy_from_slice(hashed_addr.as_slice());
                rec[32..64].copy_from_slice(entry.key.as_slice());
                rec[64..96].copy_from_slice(&entry.value.to_be_bytes::<32>());
                let mut key = [0u8; 64];
                key[..32].copy_from_slice(hashed_addr.as_slice());
                key[32..].copy_from_slice(entry.key.as_slice());
                sink.push(&rec, &key);
                if sink.part_full() {
                    break;
                }
                pair = cur.next()?;
            }
            let at_end = !sink.part_full();
            drop(cur);
            drop(tx);
            if at_end {
                break;
            }
            sink.flush(block, false)?;
        }
    }
    let (n_slots, parts_slots) = if want("slots") { sink.finish(block)? } else { (0, 0) };

    // ---- codes: variable records over Bytecodes ------------------------
    let (mut sink, done) = Sink::open(&s3, "codes", args.part_bytes, args.codes_index_every, block)?;
    if want("codes") && !done {
        loop {
            let tx = env.tx()?;
            let mut cur = tx.cursor_read::<tables::Bytecodes>()?;
            let mut pair = match &sink.last_key {
                None => cur.first()?,
                Some(k) => {
                    let kk = B256::from_slice(k);
                    match cur.seek(kk)? {
                        Some((fk, _)) if fk == kk => cur.next()?,
                        other => other,
                    }
                }
            };
            while let Some((hash, code)) = pair {
                let bytes = code.original_bytes();
                let got = keccak256(&bytes);
                eyre::ensure!(
                    got == hash,
                    "bytecode {hash:#x} hashes to {got:#x} — refusing to export a corrupt record"
                );
                let mut rec = Vec::with_capacity(36 + bytes.len());
                rec.extend_from_slice(hash.as_slice());
                rec.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
                rec.extend_from_slice(&bytes);
                sink.push(&rec, hash.as_slice());
                if sink.part_full() {
                    break;
                }
                pair = cur.next()?;
            }
            let at_end = !sink.part_full();
            drop(cur);
            drop(tx);
            if at_end {
                break;
            }
            sink.flush(block, false)?;
        }
    }
    let (n_codes, parts_codes) = if want("codes") { sink.finish(block)? } else { (0, 0) };

    if args.only.is_some() {
        eprintln!("table complete; run --finalize once all three tables are done");
        return Ok(());
    }
    // meta.json LAST: readers treat its presence as "base complete".
    let meta = serde_json::to_vec_pretty(&serde_json::json!({
        "v": 2,
        "block": block,
        "accounts": n_accounts,
        "slots": n_slots,
        "codes": n_codes,
        "parts": {"accounts": parts_accounts, "slots": parts_slots, "codes": parts_codes},
        "record": {"accounts": 104, "slots": 96},
        "keyed": "hashed",
        "part_bytes": args.part_bytes,
        "index_every": args.index_every,
        "codes_index_every": args.codes_index_every,
    }))?;
    s3.put("meta.json", &meta)?;
    eprintln!("done at block {block}");
    Ok(())
}
