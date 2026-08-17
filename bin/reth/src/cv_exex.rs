//! chainvisor semantic delta emitter (`CV_EXEX_DELTA_SPOOL`).
//!
//! An Execution Extension that writes one CVSD v1 segment per canonical
//! block into a spool directory. A host-side uploader ships spool files to
//! the object store, where readers tail them. The wire format is defined
//! by `cvsd/src/record.rs` in the chainvisor repo; this emitter and the
//! RPC-derived one there produce the same records, so a reader cannot tell
//! which one fed it.
//!
//! ## Why this exists when an RPC emitter already works
//!
//! `trace_replayBlockTransactions` can only see state that TRANSACTIONS
//! changed. Post-Prague, some state is written by system calls outside any
//! transaction (EIP-4788 beacon roots, EIP-2935 block hashes, EIP-7002 and
//! EIP-7251 request queues), and withdrawals credit balances outside
//! transactions too. This ExEx reads the node's own `ExecutionOutcome`,
//! which contains every change from every source — so its segments have no
//! declared gaps, and it costs the node nothing to produce because the
//! block was just executed.
//!
//! ## Inert unless configured
//!
//! Installed only when `CV_EXEX_DELTA_SPOOL` is set (see `main.rs`), so a
//! build carrying this code behaves exactly like one without it until an
//! operator opts in.
//!
//! ## It must never take the node down
//!
//! An ExEx that returns `Err` kills the node. This is an OBSERVER: a full
//! spool disk is not a reason to stop a writer whose whole job is to keep
//! producing chain data. So a write failure is logged loudly every time,
//! and after `PARK_AFTER` consecutive failures the emitter stops writing
//! and says exactly what an operator must do — while still acknowledging
//! `FinishedHeight` so the ExEx WAL cannot grow without bound. The stream
//! going quiet is visible to every reader as a stalled head; it is not a
//! silent workaround.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use futures_util::StreamExt;
use reth_ethereum_primitives::EthPrimitives;
use reth_execution_types::{Chain, ExecutionOutcome};
use reth_exex::{ExExContext, ExExEvent};
use reth_node_api::{FullNodeComponents, NodeTypes};
use revm_primitives::{Address, Bytes, B256, KECCAK_EMPTY, U256};
use serde_json::{json, Value};
use tracing::{error, info, warn};

/// Consecutive spool failures after which the emitter stops trying.
const PARK_AFTER: u32 = 50;

/// CVSD wire version. Must match `cvsd::record::WIRE_V`.
const WIRE_V: u32 = 1;

pub(crate) async fn run<Node>(mut ctx: ExExContext<Node>, dir: PathBuf) -> eyre::Result<()>
where
    Node: FullNodeComponents<Types: NodeTypes<Primitives = EthPrimitives>>,
{
    // The only failure that is allowed to abort start-up: if the spool
    // directory cannot exist at all, the operator misconfigured the mount
    // and should hear about it before the node starts serving.
    std::fs::create_dir_all(&dir)?;
    info!(target: "cv::exex", dir = %dir.display(), wire_v = WIRE_V, "cv-delta emitter started");

    let mut failures: u32 = 0;
    let mut parked = false;
    let mut emitted: u64 = 0;

    while let Some(result) = ctx.notifications.next().await {
        let notification = match result {
            Ok(n) => n,
            Err(e) => {
                // Losing the notification stream is not something an
                // observer can repair; report and stop observing.
                error!(target: "cv::exex", error = %e, "notification stream failed; emitter stopping");
                return Ok(());
            }
        };

        if let Some(reverted) = notification.reverted_chain() {
            if !parked {
                let range = reverted.range();
                match write_revert(&dir, &reverted) {
                    Ok(bytes) => warn!(
                        target: "cv::exex", ?range, bytes,
                        "cv-delta: emitted revert marker (reorg)"
                    ),
                    Err(e) => {
                        failures += 1;
                        error!(target: "cv::exex", ?range, error = %format!("{e:#}"), "cv-delta: revert spool write failed");
                    }
                }
            }
        }

        if let Some(committed) = notification.committed_chain() {
            let tip = committed.tip().num_hash();
            if !parked {
                let started = Instant::now();
                match emit_chain(&dir, &committed) {
                    Ok(stats) => {
                        failures = 0;
                        emitted += stats.blocks;
                        info!(
                            target: "cv::exex",
                            range = ?committed.range(),
                            blocks = stats.blocks,
                            accounts = stats.accounts,
                            slots = stats.slots,
                            code = stats.code,
                            bytes = stats.bytes,
                            build_ms = started.elapsed().as_millis() as u64,
                            block_age_secs = now_unix().saturating_sub(committed.tip().header().timestamp),
                            total_emitted = emitted,
                            "cv-delta: emitted segments"
                        );
                    }
                    Err(e) => {
                        failures += 1;
                        error!(
                            target: "cv::exex",
                            range = ?committed.range(),
                            consecutive_failures = failures,
                            error = %format!("{e:#}"),
                            "cv-delta: spool write failed"
                        );
                        if failures >= PARK_AFTER {
                            parked = true;
                            error!(
                                target: "cv::exex",
                                dir = %dir.display(),
                                "cv-delta PARKED after {PARK_AFTER} consecutive failures. The node is \
                                 UNAFFECTED and keeps committing blocks, but the semantic delta \
                                 stream has stopped and every reader will see its head stall. \
                                 Operator: check free space and permissions on the spool directory, \
                                 confirm the uploader is draining it, then restart the guest to \
                                 resume emitting."
                            );
                        }
                    }
                }
            }
            // Always acknowledged, parked or not: this is what lets the
            // ExEx WAL prune. Withholding it would turn a stopped emitter
            // into unbounded disk growth on the writer.
            ctx.events.send(ExExEvent::FinishedHeight(tip))?;
        }
    }
    Ok(())
}

#[derive(Default)]
struct Stats {
    blocks: u64,
    accounts: usize,
    slots: usize,
    code: usize,
    bytes: usize,
}

/// A block boundary's view of the accounts and slots the chain touched.
/// `accounts` maps to `None` for an account that does not exist at this
/// boundary, which the reader needs in order to distinguish an absent
/// account from an empty one.
#[derive(Default)]
struct Snapshot {
    accounts: HashMap<Address, Option<AcctFields>>,
    slots: HashMap<(Address, U256), U256>,
}

#[derive(Clone, PartialEq, Eq)]
struct AcctFields {
    balance: U256,
    nonce: u64,
    code_hash: B256,
}

/// State entering the chain: every touched key's ORIGINAL value.
fn snapshot_original<R>(outcome: &ExecutionOutcome<R>) -> Snapshot {
    let mut s = Snapshot::default();
    for (addr, acct) in &outcome.bundle.state {
        s.accounts.insert(
            *addr,
            acct.original_info.as_ref().map(|i| AcctFields {
                balance: i.balance,
                nonce: i.nonce,
                code_hash: i.code_hash,
            }),
        );
        for (key, slot) in &acct.storage {
            s.slots
                .insert((*addr, *key), slot.previous_or_original_value);
        }
    }
    s
}

/// State after the outcome's last block.
fn snapshot_post<R>(outcome: &ExecutionOutcome<R>) -> Snapshot {
    let mut s = Snapshot::default();
    for (addr, acct) in &outcome.bundle.state {
        s.accounts.insert(
            *addr,
            acct.info.as_ref().map(|i| AcctFields {
                balance: i.balance,
                nonce: i.nonce,
                code_hash: i.code_hash,
            }),
        );
        for (key, slot) in &acct.storage {
            s.slots.insert((*addr, *key), slot.present_value);
        }
    }
    s
}

/// Emit one segment per block in the chain.
///
/// A notification at tip carries a single block, and then the aggregate
/// bundle IS that block's delta: original values on one side, present
/// values on the other. During a multi-block notification (pipeline sync)
/// each block boundary is reconstructed with `execution_outcome_at_block`,
/// which reverts a clone of the bundle to that height — the same mechanism
/// reth uses to write its own per-block changesets.
fn emit_chain(dir: &Path, chain: &Chain<EthPrimitives>) -> eyre::Result<Stats> {
    let range = chain.range();
    let (first, tip) = (*range.start(), *range.end());
    let aggregate = chain.execution_outcome();
    let original = snapshot_original(aggregate);
    let mut stats = Stats::default();

    for block in chain.blocks_iter() {
        let n = block.header().number;
        let (pre, post) = if first == tip {
            (&original, snapshot_post(aggregate))
        } else {
            let at_n = chain
                .execution_outcome_at_block(n)
                .ok_or_else(|| eyre::eyre!("no execution outcome at block {n}"))?;
            (&original, snapshot_post(&at_n))
        };
        // For a multi-block chain the previous boundary is block n-1; keys
        // it never touched keep their original value, which `original`
        // supplies.
        let prev = if first == tip || n == first {
            None
        } else {
            chain
                .execution_outcome_at_block(n - 1)
                .map(|o| snapshot_post(&o))
        };

        let mut seg_stats = Stats::default();
        let delta = build_delta(pre, prev.as_ref(), &post, aggregate, &mut seg_stats);
        let header = block.header();
        let record = json!({
            "v": WIRE_V,
            "kind": "commit",
            "block": n,
            "hash": format!("{:#x}", block.hash()),
            "parent": format!("{:#x}", header.parent_hash),
            "ts": header.timestamp,
            "src": "exex",
            "emit_unix": now_unix(),
            "header": {
                "gas_limit": header.gas_limit,
                "basefee": header.base_fee_per_gas.unwrap_or(0),
                "prevrandao": format!("{:#x}", header.mix_hash),
                "beneficiary": format!("{:#x}", header.beneficiary),
                "excess_blob_gas": header.excess_blob_gas,
            },
            "delta": delta,
            "unmodeled": Vec::<String>::new(),
            "unmodeled_addrs": Vec::<String>::new(),
        });
        stats.bytes += write_spool(dir, &format!("cvsd~v1~blk~{n:012}.json"), &record)?;
        stats.blocks += 1;
        stats.accounts += seg_stats.accounts;
        stats.slots += seg_stats.slots;
        stats.code += seg_stats.code;
    }
    Ok(stats)
}

/// Per-field account and slot changes between two boundaries. A field is
/// emitted only when it actually moved: the reader treats an absent field
/// as "unchanged in this block" and resolves it from its base, so emitting
/// an unchanged pair would be redundant, and emitting a guess would be
/// wrong.
fn build_delta<R>(
    original: &Snapshot,
    prev: Option<&Snapshot>,
    post: &Snapshot,
    aggregate: &ExecutionOutcome<R>,
    stats: &mut Stats,
) -> Value {
    let pre_acct = |addr: &Address| -> Option<AcctFields> {
        match prev {
            Some(p) => match p.accounts.get(addr) {
                Some(v) => v.clone(),
                // Untouched up to the previous boundary, so still original.
                None => original.accounts.get(addr).cloned().flatten(),
            },
            None => original.accounts.get(addr).cloned().flatten(),
        }
    };
    let pre_slot = |k: &(Address, U256)| -> U256 {
        match prev.and_then(|p| p.slots.get(k)) {
            Some(v) => *v,
            None => original.slots.get(k).copied().unwrap_or(U256::ZERO),
        }
    };

    let mut accounts = Vec::new();
    let mut needed_code: BTreeMap<B256, Bytes> = BTreeMap::new();
    // Deterministic order so identical state produces identical bytes.
    let mut addrs: Vec<&Address> = post.accounts.keys().collect();
    addrs.sort();
    for addr in addrs {
        let after = post.accounts.get(addr).cloned().flatten();
        let before = pre_acct(addr);
        if before == after {
            continue;
        }
        let mut entry = json!({
            "addr": format!("{addr:#x}"),
            "exists": [before.is_some(), after.is_some()],
        });
        let b = before.clone().unwrap_or(EMPTY_FIELDS);
        let a = after.clone().unwrap_or(EMPTY_FIELDS);
        if b.balance != a.balance {
            entry["balance"] = json!([hex_u256(b.balance), hex_u256(a.balance)]);
        }
        if b.nonce != a.nonce {
            entry["nonce"] = json!([b.nonce, a.nonce]);
        }
        if b.code_hash != a.code_hash {
            entry["code_hash"] =
                json!([format!("{:#x}", b.code_hash), format!("{:#x}", a.code_hash)]);
            for h in [b.code_hash, a.code_hash] {
                if let Some(bc) = aggregate.bundle.contracts.get(&h) {
                    needed_code.insert(h, Bytes::copy_from_slice(bc.original_byte_slice()));
                }
            }
        }
        accounts.push(entry);
        stats.accounts += 1;
    }

    let mut by_addr: BTreeMap<Address, Vec<Value>> = BTreeMap::new();
    let mut keys: Vec<&(Address, U256)> = post.slots.keys().collect();
    keys.sort();
    for k in keys {
        let after = post.slots.get(k).copied().unwrap_or(U256::ZERO);
        let before = pre_slot(k);
        if before == after {
            continue;
        }
        by_addr.entry(k.0).or_default().push(json!({
            "key": hex_u256(k.1),
            "pre": hex_u256(before),
            "post": hex_u256(after),
        }));
        stats.slots += 1;
    }

    stats.code += needed_code.len();
    json!({
        "accounts": accounts,
        "storage": by_addr.into_iter().map(|(addr, slots)| json!({
            "addr": format!("{addr:#x}"),
            "slots": slots,
        })).collect::<Vec<_>>(),
        "code": needed_code.into_iter().map(|(hash, code)| json!({
            "hash": format!("{hash:#x}"),
            "code": format!("0x{}", hex_bytes(&code)),
        })).collect::<Vec<_>>(),
    })
}

/// A non-existent account reads as zero balance, zero nonce and empty
/// code. That is the EVM's definition, not a stand-in value.
const EMPTY_FIELDS: AcctFields = AcctFields {
    balance: U256::ZERO,
    nonce: 0,
    code_hash: KECCAK_EMPTY,
};

/// A reorg marker. The reader rewinds to `first - 1` and re-tails; it does
/// not need undo values, because every block from `first` onward will be
/// republished and re-applied in order.
fn write_revert(dir: &Path, chain: &Chain<EthPrimitives>) -> eyre::Result<usize> {
    let range = chain.range();
    let record = json!({
        "v": WIRE_V,
        "kind": "revert",
        "block": *range.end(),
        "first": *range.start(),
        "hash": format!("{:#x}", chain.tip().hash()),
        "parent": format!("{:#x}", chain.tip().header().parent_hash),
        "ts": chain.tip().header().timestamp,
        "src": "exex",
        "emit_unix": now_unix(),
        "header": {
            "gas_limit": chain.tip().header().gas_limit,
            "basefee": chain.tip().header().base_fee_per_gas.unwrap_or(0),
            "prevrandao": format!("{:#x}", chain.tip().header().mix_hash),
            "beneficiary": format!("{:#x}", chain.tip().header().beneficiary),
        },
        "delta": {"accounts": [], "storage": [], "code": []},
    });
    write_spool(dir, &format!("cvsd~v1~blk~{:012}.json", *range.start()), &record)
}

/// Atomic publish: temp file, fsync, rename. The uploader only ever sees
/// whole segments. The file name encodes the object key with `/` written
/// as `~`, so the uploader needs no knowledge of the key layout.
fn write_spool(dir: &Path, name: &str, record: &Value) -> eyre::Result<usize> {
    let body = serde_json::to_vec(record)?;
    let tmp = dir.join(format!(".tmp-{name}"));
    std::fs::write(&tmp, &body)?;
    let f = std::fs::File::open(&tmp)?;
    f.sync_all()?;
    drop(f);
    std::fs::rename(&tmp, dir.join(name))?;
    Ok(body.len())
}

fn hex_u256(v: U256) -> String {
    format!("{v:#x}")
}

fn hex_bytes(b: &[u8]) -> String {
    let mut s = String::with_capacity(b.len() * 2);
    for byte in b {
        s.push_str(&format!("{byte:02x}"));
    }
    s
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
