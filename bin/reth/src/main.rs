#![allow(missing_docs)]

#[global_allocator]
static ALLOC: reth_cli_util::allocator::Allocator = reth_cli_util::allocator::new_allocator();

// Required for "override_allocator_on_supported_platforms".
#[cfg(all(feature = "jemalloc", unix))]
use reth_cli_util::allocator::tikv_jemalloc_sys as _;

#[cfg(all(feature = "jemalloc-prof", unix))]
#[unsafe(export_name = "malloc_conf")]
static MALLOC_CONF: &[u8] = b"prof:true,prof_active:true,lg_prof_sample:19\0";

use clap::Parser;
use reth::cli::Cli;
use reth_ethereum_cli::chainspec::EthereumChainSpecParser;
use reth_node_ethereum::EthereumNode;
use tracing::info;

/// chainvisor semantic delta emitter. Installed only when
/// `CV_EXEX_DELTA_SPOOL` is set, so this module is inert in every build
/// that does not opt in.
mod cv_exex;

fn main() {
    reth_cli_util::sigsegv_handler::install();

    // chainvisor FlushInPlace quiesce shield: when the writer (chainvisor, our
    // parent in the same container) is configured with
    // CV_SNAPSHOT_QUIESCE=flush-in-place it sends us SIGUSR1 before every LVM
    // snapshot and waits for the marker file. The flush handler proper only
    // arms once the provider is up (launch/engine.rs, gated on the same env
    // var) — but the default SIGUSR1 disposition TERMINATES the process, so a
    // quiesce landing during the (minutes-long) cold-open / RocksDB WAL replay
    // killed the guest on every snapshot tick (measured 2026-07-02:
    // `guest exited s=ExitStatus(unix_wait_status(10))` in a restart loop).
    // Ignore SIGUSR1 from the first instruction; tokio's signal stream
    // replaces the disposition when the real handler arms. Until then the
    // writer's marker wait times out and it aborts that snapshot loudly —
    // exactly the intended "failed quiesce must abort" semantics.
    #[cfg(unix)]
    if std::env::var("CV_RETH_FLUSH_ON_SIGUSR1").as_deref() == Ok("1") {
        unsafe { libc::signal(libc::SIGUSR1, libc::SIG_IGN) };
    }

    // Enable backtraces unless a RUST_BACKTRACE value has already been explicitly provided.
    if std::env::var_os("RUST_BACKTRACE").is_none() {
        unsafe { std::env::set_var("RUST_BACKTRACE", "1") };
    }

    if let Err(err) = Cli::<EthereumChainSpecParser>::parse().run(async move |builder, _| {
        info!(target: "reth::cli", "Launching node");

        // chainvisor semantic delta stream (CVSD). The ExEx writes one
        // segment per committed block into this directory; a host-side
        // uploader ships them to the object store, where userspace readers
        // tail them. Unset means not installed at all — no notification
        // subscription, no ExEx WAL, no behaviour change whatsoever.
        let spool = std::env::var("CV_EXEX_DELTA_SPOOL").ok();
        let cv_delta_enabled = spool.is_some();
        if cv_delta_enabled {
            info!(target: "reth::cli", spool = ?spool, "installing chainvisor cv-delta ExEx");
        }

        let handle = builder
            .node(EthereumNode::default())
            .install_exex_if(cv_delta_enabled, "cv-delta", move |ctx| {
                let dir = std::path::PathBuf::from(spool.clone().unwrap_or_default());
                async move { Ok(crate::cv_exex::run(ctx, dir)) }
            })
            .launch_with_debug_capabilities()
            .await?;

        handle.wait_for_node_exit().await
    }) {
        eprintln!("Error: {err:?}");
        std::process::exit(1);
    }
}
