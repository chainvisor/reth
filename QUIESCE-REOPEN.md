# In-process quiesce/reopen (chainvisor guest contract G1/G2, exit-free)

Goal: survive a chainvisor base swap without process exit — release every
filesystem hold on the mount, wait for `.swap-done`, reopen, jump head.
Replaces the guest-exit form's respawn (+~15 s pause, full cache loss).

## Protocol (files next to CV_ADOPT_ANCHOR_PATH; watch lives in
## AdoptAnchorWatch, already polled from on_new_payload — f50c03b)
- reader writes `<anchor>.quiesce` (content: candidate epoch) →
- reth: (1) engine returns SYNCING for new payloads; (2) drain in-flight
  tree work; (3) gate RPC state access (brief 503s are fine — the public
  gates are closed while stale anyway); (4) static files: drop every jar
  mmap (clear the jar map — the 6aac9da force-refresh machinery re-scans
  on reopen); (5) MDBX: close the env via the swappable wrapper (below);
  (6) write `<anchor>.quiesce.ack` →
- reader: umount → COW reset → pointer swap → mount → `<anchor>.swap-done` →
- reth: reopen env at the same path, re-scan static files, read the DB
  head (the boot-adopt logic, in place), resume payloads; delete the
  sidecars.

## The MDBX swappable wrapper (the one real surgery)
reth-db `DatabaseEnv { inner: Environment }` becomes
`inner: parking_lot::RwLock<Option<Environment>>` behind the SAME public
API: `tx()`/`tx_mut()` take a read lock, clone what they need, and error
with a typed `EnvQuiesced` while `None` (callers already handle db
errors; the engine is drained and RPC gated, so in practice no caller
hits it). Quiesce: take the write lock (waits for all in-flight
tx-creation), `take()` + drop (mdbx closes, mmaps unmapped). Reopen:
rebuild `Environment` with the boot-time builder args (retained in the
wrapper at first open). Perf: one uncontended parking_lot read-lock per
txn OPEN (not per op) — noise against mdbx txn cost.

## Sequencing invariants (from the exit-form lessons, all measured)
- Never during initial election; the swap supersedes the advance lane
  (cancel+clear) — both already enforced controller-side.
- Correlation uniqueness for any new wire form (chunk_idx in shard/slot).
- The teardown holder-grace applies unchanged (no process exit at all).

## Kill-experiment
CV_QUIESCE_SWAP_INPROCESS=1 (reader chooses sidecar protocol instead of
terminate): (a) pause ≤3 s wall (ack→swap-done→resumed); (b) reth RSS and
trie caches survive (RSS continuity across swap); (c) byte-verify at the
post-swap head vs writer; (d) per-epoch swap cadence sustainable:
freshness floor ≈ epoch age (~120-150 s) with zero restarts across ≥20
consecutive swaps; (e) with CES warm, tail execution at ~1.5 s/block
closes to gossip head — criterion-1 seconds within reach.
