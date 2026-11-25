#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::significant_drop_tightening))]
mod listeners;
pub mod metrics;
mod order_book;
mod prelude;
mod servers;
mod types;

use std::path::PathBuf;

use clap::ValueEnum;

pub use order_book::market_filter::MarketFilter;
pub use prelude::Result;
pub use servers::websocket_server::run_websocket_server;

/// Snapshot fetching mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Default)]
pub enum SnapshotMode {
    /// Use docker exec to call hl-node inside container
    #[default]
    Docker,
    /// Call hl-node directly (for systemctl/bare metal setups)
    Direct,
}

/// Server configuration passed from CLI arguments
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Full address string (e.g., "0.0.0.0:8000")
    pub address: String,
    /// WebSocket compression level (0-9)
    pub compression_level: u32,
    /// Optional base directory for hlnode data
    pub data_dir: Option<PathBuf>,
    /// Markets this instance tracks; events for every other market are skipped
    pub markets: MarketFilter,
    /// Snapshot fetching mode (docker or direct)
    pub snapshot_mode: SnapshotMode,
    /// Docker container name for exec commands (docker mode only)
    pub docker_container: String,
    /// Path to hl-node binary (direct mode only)
    pub hlnode_binary: String,
    /// Path to abci_state.rmp file (direct mode only, has default)
    pub abci_state_path: Option<PathBuf>,
    /// Path where snapshot will be written (direct mode only, has default)
    pub snapshot_output_path: Option<PathBuf>,
    /// Path to visor_abci_state.json (optional)
    pub visor_state_path: Option<PathBuf>,
    /// Port for Prometheus metrics endpoint (0 to disable)
    pub metrics_port: u16,
    /// BBO-only mode: lightweight mode that only tracks best bid/ask per coin
    /// Disables L2/L4/Trades subscriptions but uses ~100MB RAM instead of 2-3GB
    pub bbo_only: bool,
    /// Resend the last l2Book payload every N ms when nothing has changed.
    /// 0 = disabled (default). Provides a heartbeat for low-liquidity coins
    /// whose snapshot hash rarely changes, matching the official HL API behavior.
    pub l2book_heartbeat_ms: u64,
    /// Resend the last bbo payload every N ms when nothing has changed.
    /// 0 = disabled (default).
    pub bbo_heartbeat_ms: u64,
    /// Tolerate drift: when true, data-loss events are counted in metrics but
    /// never trigger a snapshot re-fetch. The book keeps serving live events
    /// through drift and does NOT self-heal until restarted. Off by default.
    pub no_resync: bool,
    /// Cap on events cached for gapless replay while a snapshot fetch is in
    /// flight (~0.5-1KB resident each). Must cover a full fetch window at
    /// peak event rate or every market-hours re-sync overflows into another
    /// re-sync. 0 = keep the built-in default. Applies to each book thread.
    pub replay_cache_events: usize,
    /// Threads that apply events. The coins are split into this many groups,
    /// each with its own books and its own thread; clients still see one
    /// server. 1 applies everything on one thread. Capped at
    /// [`MAX_BOOK_THREADS`].
    pub book_threads: usize,
}

/// The most book threads a server will run.
pub const MAX_BOOK_THREADS: usize = listeners::order_book::MAX_BOOK_THREADS;
