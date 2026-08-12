# FENCE-TODO — commits NOT carried onto reth v2.5.0

Branch `chainvisor-v2.5.0` = tag `v2.5.0` + the **21** chainvisor commits that
carry cleanly. **18** of the 39 commits in `v2.2.0..chainvisor-v2.2.0` are
deliberately **UNAPPLIED**. This file is the worklist for the fence
re-derivation session.

Nothing here is lost: every commit is still reachable on `chainvisor-v2.2.0`.
This pass carried the **capture surface** (libmdbx uprobe markers, nippy-jar
offset probe, static-file / nippy-jar read identification) and the standalone
fixes. It deliberately did **not** attempt the persistence-fence family, the
trusting-reader adopt family, or the force-at-tip family.

## Ground rules used in this pass

Anything that conflicted was **skipped, not resolved** — except three commits
whose conflicts were pure *additive unions* (both sides add independent items
at the same anchor; the resolution keeps 100% of both sides and inserts only
a closing brace):

| Commit | File | Resolution |
|---|---|---|
| `14ffd2487` | `crates/engine/primitives/src/config.rs` | 4 hunks — upstream's `skip_state_root` / `num_state_masking_blocks` vs our `min_blocks_for_pipeline_run`; kept both |
| `8e74bf6b2` | `crates/rpc/rpc-eth-api/src/helpers/spec.rs` | 2 hunks — upstream's `effective_resource` + its tests vs our `should_report_syncing` + its test; kept both |
| `061973eb0` | — | cascaded in clean once `8e74bf6b2` landed |

Two notes for whoever picks this up:

- **`14ffd2487` must be resolved FIRST.** It is the config foundation. Skipping
  it makes every downstream force-at-tip commit conflict spuriously — the
  first attempt at this port produced 20 conflicts; resolving `14ffd2487`
  alone dropped that to 17.
- **`5da7529a5` was assessed as "one import line" — it is not.** Its
  `launch/engine.rs` hunk is a 127-line insertion against an *empty* HEAD
  side. It was skipped; see its row below.

## Conflict classes

- **A — missing-dependency (empty context).** The HEAD side of the hunk is
  *empty*: the code our patch edits was introduced by an earlier **skipped**
  commit. Do not hand-write these; they resolve for free once their parent
  lands. Re-test after each parent.
- **B — upstream rewrite.** Both sides substantial; reth v2.5.0 restructured
  the same region. Needs semantic re-derivation against the new upstream
  shape, not textual merging.
- **C — additive union.** Small on both sides, mechanical — but gated behind
  a class-A/B parent in the same family.
- **D — no conflict, dropped for coherence.** Applied cleanly but calls a
  symbol defined only in a skipped commit. Kept out so the branch compiles.

## The worklist (apply in this order — it is the original commit order)

The 4 commits called out in the upgrade plan are marked **★**.

| # | Commit | Subject | Files | Conflicts (hunks, largest HEAD-vs-ours) | Class |
|---|---|---|---|---|---|
| 1 | **★ `6db732481`** | chainvisor: allow disabling engine persistence | `engine/primitives/src/config.rs`, `engine/tree/src/tree/mod.rs`, `engine/tree/src/tree/tests.rs`, `node/core/src/args/engine.rs` | config.rs (4, 3v3); tree/mod.rs (2, 6v28); tests.rs (3, 18v19); args/engine.rs (3, 3v3) | C + **B** on tree/mod.rs |
| 2 | `948e1a2fc` | Keep force-at-tip backfill state nonblocking | config.rs, tree/mod.rs, tests.rs | tree/mod.rs (2, 6v28) | B |
| 3 | `9c91484b5` | Keep force-at-tip canonical head advancing | tree/mod.rs, tests.rs | tests.rs (1, 2v87) | B |
| 4 | `5b29f9273` | chainvisor: commit force-at-tip linear extensions on downloaded blocks | tree/mod.rs | **none** | **D** — see below |
| 5 | `ed3c5825b` | trusting-reader ADOPT — adopt writer-committed on-disk blocks | tree/mod.rs | tree/mod.rs (2, 1v17) | B |
| 6 | `7f45e336b` | trusting-reader wait-for-commit (close the sawtooth) | config.rs, tree/mod.rs, args/engine.rs | tree/mod.rs (1, **0v110**) | **A** (needs #5) |
| 7 | **★ `eb91fa9d4`** | trusting-reader pure-adopt — RDONLY MDBX + head poller | `cli/commands/src/node.rs`, config.rs, tree/mod.rs, `node/builder/src/launch/common.rs`, `launch/engine.rs`, args/engine.rs | node.rs (1, 3v14); config.rs (5, 0v2); tree/mod.rs (1, 1v32); common.rs (4, **65v19**); args/engine.rs (5, 2v6) | **A + B** |
| 8 | `d8e814d1d` | trusting-reader static files stay READ-WRITE | launch/common.rs | common.rs (1, 21v21) | B |
| 9 | **★ `d3a583cf1`** | rocksdb: trust-based repair on torn-snapshot corruption | `storage/provider/src/providers/rocksdb/provider.rs` | provider.rs (1, 7v5) | B (small) |
| 10 | `d661e06eb` | rocksdb: FlushInPlace snapshot-quiesce (gated, default off) | launch/engine.rs, rocksdb/provider.rs | engine.rs (2, 1v1) | C |
| 11 | `f37f515ea` | wire `read_only_sync` for the trusting-reader node | launch/common.rs | common.rs (2, 22v29) | B |
| 12 | `884992ad3` | trusting-reader monotonic head guard | launch/engine.rs | engine.rs (1, **0v112**) | **A** (needs #7) |
| 13 | **★ `3988bc089`** | fence pipeline and persistence ownership | 16 files incl. `engine/tree/src/persistence.rs`, `persistence_fence.rs` (new), `launch.rs`, `tree/mod.rs`, `tree/tests.rs`, `launch/common.rs`, `launch/engine.rs`, `storage/provider/src/test_utils/mock.rs` | persistence.rs (13, **77v4**); tree/mod.rs (12, 7v10); tests.rs (12, 15v17); common.rs (4, **111v4**); engine.rs (4, 1v32); mock.rs (8, 10v0); launch.rs (3, 2v1); error.rs (2, 1v1) | **B — the fence root; largest job** |
| 14 | `5da7529a5` | fence snapshots across provider commits | launch/engine.rs, `blockchain_provider.rs`, `database/mod.rs`, `database/provider.rs` | engine.rs (1, **0v127**); database/provider.rs (1, 1v1) | **A** (needs #7 + #13) |
| 15 | `be040d90e` | fail closed on snapshot barrier escapes | launch/engine.rs, database/mod.rs, database/provider.rs | engine.rs (1, **0v129**); database/mod.rs (1, **0v134**) | **A** (needs #14) |
| 16 | `fcb8466f7` | make raw transaction fence generic | database/provider.rs | provider.rs (2, 1v1) | C (needs #14) |
| 17 | `a9cc50cd5` | continue bounded persistence convergence | `persistence_fence.rs`, `tree/error.rs`, tree/mod.rs, tests.rs | persistence_fence.rs (**file absent** — created by #13); error.rs (1, 0v52); tree/mod.rs (3, 0v6); tests.rs (1, 6v204) | **A** (needs #13) |
| 18 | `f50c03bf6` | G2 adopt-anchoring — jump canonical head to advanced base's anchor | tree/mod.rs | tree/mod.rs (2, 1v44) | B (needs #5–#7) |

### Why `5b29f9273` (#4) is here despite applying cleanly

It cherry-picks without conflict, but its only change is a call to
`self.try_commit_force_at_tip_linear_extension(...)` — a method defined by
`9c91484b5` (#3), which is skipped. That method in turn calls
`self.config.pipeline_backfill_disabled()`, defined by `948e1a2fc` (#2), also
skipped. Keeping #4 alone breaks the build (`E0599`), so it was applied,
found dangling by `cargo check`, and dropped. Re-apply it with #2 and #3.

## Dependency shape

Three families, each rooted in a class-B commit that must be re-derived by
hand before its class-A children apply for free:

- **force-at-tip / persistence toggle** — root `6db732481`; then #2, #3, #4.
- **trusting-reader adopt** — root `ed3c5825b` → `7f45e336b` → `eb91fa9d4`;
  then #8, #10, #11, #12, #18. `eb91fa9d4` is the RDONLY-MDBX + head-poller
  commit and carries the biggest single divergence outside the fence.
- **persistence fence** — root `3988bc089`; then #14, #15, #16, #17.

`d3a583cf1` (rocksdb repair, one file, 1 hunk) is independent of all three
and is the cheapest of the four ★ commits.

## Known compile follow-up (not yet needed on this branch)

In v2.5.0 `EngineArgs`, `persistence_backpressure_threshold` and
`memory_block_buffer_target` are now **`Option<u64>`** (were `u64`) —
`crates/node/core/src/args/engine.rs:327,349`. No commit in the applied set
constructs them, so this branch compiles as-is. The commits that DO touch
those sites are unapplied: `6db732481` (#1) and `eb91fa9d4` (#7) both patch
`args/engine.rs`. Thread `Some(...)` when re-deriving them.

## Reproducing this pass

```sh
git checkout -b chainvisor-v2.5.0 v2.5.0
for sha in $(git rev-list --reverse --no-merges v2.2.0..chainvisor-v2.2.0); do
  git cherry-pick -x "$sha" || git cherry-pick --abort   # skip on conflict
done
```

Resolve `14ffd2487` first, then re-run the loop to pick up the cascade.

## Verification state of this branch

- `cargo check -p reth-node-core -p reth-nippy-jar -p reth-libmdbx
  -p reth-engine-tree -p reth-rpc-eth-api` — **passes** (exit 0, rustc
  1.97.1, workspace `rust-version = 1.95`). Those are the crates the applied
  set touches, plus the two carrying hand-resolved conflicts.
- One pre-existing dead-code warning: `MIN_BLOCKS_FOR_PIPELINE_RUN` is never
  used in `reth-engine-tree`'s lib build. This is **not** a porting defect —
  `14ffd2487` replaces its only non-test use with the configurable
  `self.config.min_blocks_for_pipeline_run()`, and `chainvisor-v2.2.0` has
  exactly the same unused constant.
- Capture surface confirmed structurally identical to `chainvisor-v2.2.0`:
  `chainvisor_mdbx_cursor_path` (declaration + call site + definition, all
  `__noinline`), `__noinline` on `page_get_three` / `page_get_large`, and
  `chainvisor_reth_nippyjar_offset_probe_v2` (one definition, one production
  call site, one symbol-retention reference, one test).
- No conflict markers anywhere on the branch.
