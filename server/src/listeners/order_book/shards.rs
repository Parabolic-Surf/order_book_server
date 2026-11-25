//! Books split into groups of coins, each group owned by one thread.
//!
//! One listener applies every event on one task, which caps the server at one
//! core. Here the coins are divided into groups by a hash of the coin name.
//! Each group has its own listener, and so its own books, lock and apply loop.
//! One reader parses the node's files and hands every event to the group that
//! owns its coin. Clients still see one server.

use std::{
    collections::HashSet,
    sync::{Arc, Mutex as StdMutex, PoisonError},
    time::Duration,
};

use alloy::primitives::Address;
use log::info;
use tokio::{
    sync::{Mutex, broadcast::Sender},
    time::Instant,
};

use super::{
    ActiveL2Params, ActiveSubs, InternalMessage, OrderBookListener, strip_untracked_events,
    utils::{EventBatch, SnapshotConfig, get_visor_path, process_rmp_file, read_visor_height},
};
use crate::{
    metrics::ORDERBOOK_READY,
    order_book::{
        Coin,
        market_filter::{MarketFilter, ShardRoute},
        multi_book::{Snapshots, load_snapshots_from_cli_json},
    },
    prelude::*,
    types::{
        L4Order,
        inner::InnerL4Order,
        node_data::EventSource,
    },
};

/// More groups than this only adds threads: the reader that feeds them becomes the limit first.
pub(crate) const MAX_BOOK_THREADS: usize = 16;

/// How long the unclaimed part of a node snapshot is kept for groups that have not taken theirs.
const DUMP_KEEP: Duration = Duration::from_secs(120);

/// Wait this long before starting a dump. Groups ask within milliseconds of
/// each other; the ones that began caching inside this window share the dump
/// instead of each waiting for one of its own.
const DUMP_SETTLE: Duration = Duration::from_millis(250);

/// Least time between the end of one dump and the start of the next. Each
/// group paces its own re-syncs; this keeps several groups from running the
/// node's dump back to back.
const DUMP_MIN_GAP: Duration = Duration::from_secs(10);

/// What the groups of one server share: each group's coins, and through them readiness.
pub(crate) struct ShardGroup {
    universes: StdMutex<Vec<Option<Arc<HashSet<String>>>>>,
}

impl ShardGroup {
    pub(crate) fn new(count: usize) -> Arc<Self> {
        Arc::new(Self { universes: StdMutex::new(vec![None; count]) })
    }

    /// Record the coins of group `index` after an install or a change in its
    /// coin set, and return the coins of all groups that have reported. The
    /// server counts as ready once every group has.
    pub(crate) fn publish(&self, index: usize, coins: Arc<HashSet<String>>) -> Arc<HashSet<String>> {
        let mut universes = self.universes.lock().unwrap_or_else(PoisonError::into_inner);
        universes[index] = Some(coins);
        if universes.iter().all(Option::is_some) {
            ORDERBOOK_READY.set(1);
        }
        Self::merge(&universes)
    }

    pub(crate) fn all_ready(&self) -> bool {
        self.universes.lock().unwrap_or_else(PoisonError::into_inner).iter().all(Option::is_some)
    }

    /// The coins of every group that has reported.
    pub(crate) fn merged(&self) -> Arc<HashSet<String>> {
        Self::merge(&self.universes.lock().unwrap_or_else(PoisonError::into_inner))
    }

    fn merge(universes: &[Option<Arc<HashSet<String>>>]) -> Arc<HashSet<String>> {
        if let [Some(only)] = universes {
            return Arc::clone(only);
        }
        Arc::new(universes.iter().flatten().flat_map(|coins| coins.iter().cloned()).collect())
    }
}

/// Every group's listener, for the code that serves clients. A request about
/// one coin goes to the group that owns it.
#[derive(Clone)]
pub(crate) struct BookShards {
    listeners: Arc<Vec<Arc<Mutex<OrderBookListener>>>>,
    group: Arc<ShardGroup>,
    markets: MarketFilter,
    active_l2_params: ActiveL2Params,
    active_subs: ActiveSubs,
}

impl BookShards {
    /// `count` groups over the coins `markets` admits. `configure` runs on each
    /// group's listener before it starts.
    pub(crate) fn new(
        count: usize,
        internal_message_tx: Option<Sender<Arc<InternalMessage>>>,
        markets: MarketFilter,
        active_l2_params: ActiveL2Params,
        configure: impl Fn(&mut OrderBookListener),
    ) -> Self {
        let count = count.clamp(1, MAX_BOOK_THREADS);
        let group = ShardGroup::new(count);
        let active_subs = ActiveSubs::default();
        let listeners = (0..count)
            .map(|index| {
                let mut listener = OrderBookListener::new_in_group(
                    internal_message_tx.clone(),
                    markets.for_shard(ShardRoute::new(index, count)),
                    active_l2_params.clone(),
                    active_subs.clone(),
                    Arc::clone(&group),
                );
                configure(&mut listener);
                Arc::new(Mutex::new(listener))
            })
            .collect();
        Self { listeners: Arc::new(listeners), group, markets, active_l2_params, active_subs }
    }

    pub(crate) fn count(&self) -> usize {
        self.listeners.len()
    }

    pub(crate) fn listeners(&self) -> &[Arc<Mutex<OrderBookListener>>] {
        &self.listeners
    }

    pub(crate) fn group(&self) -> &Arc<ShardGroup> {
        &self.group
    }

    /// The market rule shared by every group.
    pub(crate) const fn markets(&self) -> &MarketFilter {
        &self.markets
    }

    /// The listener of the group that owns `coin`.
    pub(crate) fn for_coin(&self, coin: &str) -> &Arc<Mutex<OrderBookListener>> {
        &self.listeners[ShardRoute::shard_of(coin, self.listeners.len())]
    }

    pub(crate) fn active_l2_params(&self) -> ActiveL2Params {
        self.active_l2_params.clone()
    }

    pub(crate) fn active_subs(&self) -> ActiveSubs {
        self.active_subs.clone()
    }

    /// True once every group has installed its first snapshot.
    pub(crate) async fn is_ready(&self) -> bool {
        for listener in self.listeners.iter() {
            if !listener.lock().await.is_ready() {
                return false;
            }
        }
        true
    }

    /// Every coin served, across all groups.
    pub(crate) async fn universe(&self) -> Arc<HashSet<String>> {
        if let [only] = self.listeners.as_slice() {
            return only.lock().await.universe();
        }
        let mut coins = HashSet::new();
        for listener in self.listeners.iter() {
            coins.extend(listener.lock().await.universe().iter().cloned());
        }
        Arc::new(coins)
    }

    /// Untriggered trigger orders as (time, height, orders): one coin's from
    /// its group, or all of them. None until every group asked is ready.
    pub(crate) async fn untriggered_snapshot(&self, coin: Option<&Coin>) -> Option<(u64, u64, Vec<Arc<InnerL4Order>>)> {
        if let Some(coin) = coin {
            return self.for_coin(coin.as_str()).lock().await.compute_untriggered_snapshot(Some(coin));
        }
        let (mut time, mut height, mut orders) = (u64::MAX, u64::MAX, Vec::new());
        for listener in self.listeners.iter() {
            let (group_time, group_height, group_orders) = listener.lock().await.compute_untriggered_snapshot(None)?;
            // Groups are read one after another. The oldest stamp is the one
            // every group has reached: a reader that resumes from it may see
            // a change twice but cannot miss one.
            time = time.min(group_time);
            height = height.min(group_height);
            orders.extend(group_orders);
        }
        Some((time, height, orders))
    }
}

/// One step for a group's apply loop.
pub(super) enum ShardEvent {
    Apply(u64, EventBatch, EventSource),
    /// Lines read from disk at startup: kept for replay, never applied live.
    Backfill(EventBatch),
    /// The reader lost data, so no group's book can be trusted.
    Desync,
}

/// Divide one parsed batch among the groups. Events for markets the server does
/// not track are dropped here, once. A group with no event in the batch still
/// gets the empty batch, so its book follows the block height; an empty batch
/// of fills carries nothing and is left out.
pub(super) fn split_for_shards(mut batch: EventBatch, markets: &MarketFilter, count: usize) -> Vec<Option<EventBatch>> {
    strip_untracked_events(&mut batch, markets);
    if count == 1 {
        return vec![Some(batch)];
    }
    match batch {
        EventBatch::Orders(batch) => batch
            .split_events(count, |status| ShardRoute::shard_of(&status.order.coin, count))
            .into_iter()
            .map(|part| Some(EventBatch::Orders(part)))
            .collect(),
        EventBatch::BookDiffs(batch) => batch
            .split_events(count, |diff| ShardRoute::shard_of(diff.coin_str(), count))
            .into_iter()
            .map(|part| Some(EventBatch::BookDiffs(part)))
            .collect(),
        EventBatch::Fills(batch) => batch
            .split_events(count, |fill| ShardRoute::shard_of(&fill.1.coin, count))
            .into_iter()
            .map(|part| (part.events_len() > 0).then(|| EventBatch::Fills(part)))
            .collect(),
    }
}

/// One group's share of a node snapshot.
pub(super) struct SnapshotPart {
    pub(super) height: u64,
    pub(super) snapshot: Snapshots<InnerL4Order>,
    pub(super) untriggered: Vec<InnerL4Order>,
    /// Whether the whole dump carried trigger orders, in any market or group.
    pub(super) dump_had_untriggered: bool,
}

/// A parsed node snapshot that groups take their coins out of.
struct SharedDump {
    started: Instant,
    height: u64,
    had_untriggered: bool,
    rest: StdMutex<DumpRest>,
}

/// What no group has taken yet.
struct DumpRest {
    snapshot: Snapshots<InnerL4Order>,
    untriggered: Vec<InnerL4Order>,
    /// Groups that have not taken their share.
    claims_left: usize,
}

impl SharedDump {
    fn take(&self, markets: &MarketFilter) -> SnapshotPart {
        let mut rest = self.rest.lock().unwrap_or_else(PoisonError::into_inner);
        let snapshot = rest.snapshot.take_coins(|coin| markets.includes_coin(coin));
        let (untriggered, others) =
            std::mem::take(&mut rest.untriggered).into_iter().partition(|order| markets.includes_coin(&order.coin));
        rest.untriggered = others;
        rest.claims_left = rest.claims_left.saturating_sub(1);
        if rest.claims_left == 0 {
            // Every group has its share; what is left belongs to markets nobody tracks.
            rest.snapshot = Snapshots::new(std::collections::HashMap::new());
            rest.untriggered = Vec::new();
        }
        SnapshotPart { height: self.height, snapshot, untriggered, dump_had_untriggered: self.had_untriggered }
    }
}

/// A loaded dump: (height, books, untriggered orders).
#[cfg(test)]
type LoadedDump = (u64, Snapshots<InnerL4Order>, Vec<InnerL4Order>);

enum DumpSource {
    /// Run the node binary and read what it writes.
    Node(SnapshotConfig),
    #[cfg(test)]
    Fixed(Box<dyn Fn() -> Result<LoadedDump> + Send + Sync>),
}

/// Takes node snapshots for all groups. A dump of the node's state is the most
/// expensive thing this server asks of the host, so groups that need one at the
/// same time share a single dump, and never more than one runs at a time.
pub(crate) struct SnapshotBroker {
    source: DumpSource,
    track_untriggered: bool,
    /// How many groups take a share of each dump.
    groups: usize,
    latest: Mutex<Latest>,
}

/// Guarded by one lock, which also makes dumps run one at a time.
#[derive(Default)]
struct Latest {
    dump: Option<Arc<SharedDump>>,
    /// When the last dump ended, whether it worked or not.
    finished: Option<Instant>,
}

impl SnapshotBroker {
    pub(super) fn new(config: SnapshotConfig, track_untriggered: bool, groups: usize) -> Arc<Self> {
        Arc::new(Self { source: DumpSource::Node(config), track_untriggered, groups, latest: Mutex::default() })
    }

    #[cfg(test)]
    pub(super) fn fixed(groups: usize, load: impl Fn() -> Result<LoadedDump> + Send + Sync + 'static) -> Arc<Self> {
        Arc::new(Self {
            source: DumpSource::Fixed(Box::new(load)),
            track_untriggered: true,
            groups,
            latest: Mutex::default(),
        })
    }

    /// The caller's share of a snapshot that is safe for it to install: one
    /// whose dump began after the caller started caching events at
    /// `caching_since`, so the cache covers everything the dump may lack.
    pub(super) async fn fetch(self: &Arc<Self>, caching_since: Instant, markets: &MarketFilter) -> Result<SnapshotPart> {
        let dump = {
            let mut latest = self.latest.lock().await;
            match latest.dump.as_ref() {
                Some(dump) if dump.started >= caching_since => Arc::clone(dump),
                _ => {
                    // Free what is left of the previous dump before loading the next.
                    latest.dump = None;
                    if let Some(finished) = latest.finished {
                        tokio::time::sleep_until(finished + DUMP_MIN_GAP).await;
                    }
                    let result = self.dump().await;
                    latest.finished = Some(Instant::now());
                    let dump = Arc::new(result?);
                    latest.dump = Some(Arc::clone(&dump));
                    self.forget_later(&dump);
                    dump
                }
            }
        };
        Ok(dump.take(markets))
    }

    async fn dump(&self) -> Result<SharedDump> {
        tokio::time::sleep(DUMP_SETTLE).await;
        // Stamped before the height is read: a group that began caching before
        // this moment has every event the dump could be missing.
        let started = Instant::now();
        let (height, snapshot, untriggered) = match &self.source {
            DumpSource::Node(config) => {
                // Read the height BEFORE the dump runs: the dump is at least
                // this fresh, so replaying from here can only repeat events,
                // which is harmless. Reading it after would skip the events
                // that arrived while the dump ran.
                let pre_dump_height = read_visor_height(&get_visor_path(config))
                    .ok_or("visor state height unavailable before dump")?;
                let output = process_rmp_file(config).await?;
                load_snapshots_from_cli_json::<InnerL4Order, (Address, L4Order)>(&output, pre_dump_height).await?
            }
            #[cfg(test)]
            DumpSource::Fixed(load) => load()?,
        };
        info!("Snapshot loaded at height {height} ({} untriggered orders)", untriggered.len());
        let had_untriggered = !untriggered.is_empty() || (self.track_untriggered && snapshot.has_triggers());
        let rest = DumpRest { snapshot, untriggered, claims_left: self.groups };
        Ok(SharedDump { started, height, had_untriggered, rest: StdMutex::new(rest) })
    }

    /// Drop the unclaimed part of `dump` after a while: a group that did not
    /// need this dump never takes its share, and parsed books are large.
    fn forget_later(self: &Arc<Self>, dump: &Arc<SharedDump>) {
        let broker = Arc::downgrade(self);
        let dump = Arc::downgrade(dump);
        tokio::spawn(async move {
            tokio::time::sleep(DUMP_KEEP).await;
            let (Some(broker), Some(dump)) = (broker.upgrade(), dump.upgrade()) else {
                return;
            };
            let mut latest = broker.latest.lock().await;
            if latest.dump.as_ref().is_some_and(|current| Arc::ptr_eq(current, &dump)) {
                latest.dump = None;
            }
        });
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::{
        order_book::{Px, Side, Snapshot, Sz},
        types::node_data::{Batch, NodeDataOrderDiff, NodeDataOrderStatus},
    };
    use std::{
        collections::HashMap,
        sync::atomic::{AtomicUsize, Ordering},
    };

    fn order(coin: &str, oid: u64) -> InnerL4Order {
        InnerL4Order {
            user: Address::new([0; 20]),
            coin: Coin::new(coin),
            side: Side::Bid,
            limit_px: Px::new(1_000),
            sz: Sz::new(100_000_000),
            oid,
            timestamp: 0,
            trigger_condition: String::new(),
            is_trigger: false,
            trigger_px: String::new(),
            is_position_tpsl: false,
            reduce_only: false,
            order_type: String::new(),
            tif: None,
            cloid: None,
        }
    }

    fn dump_of(coins: &[&str]) -> LoadedDump {
        let books = coins
            .iter()
            .enumerate()
            .map(|(index, coin)| (Coin::new(coin), Snapshot::from_sides(vec![order(coin, index as u64 + 1)], Vec::new())))
            .collect::<HashMap<_, _>>();
        let untriggered = coins.iter().enumerate().map(|(index, coin)| order(coin, 1_000 + index as u64)).collect();
        (42, Snapshots::new(books), untriggered)
    }

    const COINS: [&str; 8] = ["BTC", "ETH", "SOL", "HYPE", "xyz:AMD", "xyz:NVDA", "io:GOLD", "@107"];

    fn status_batch(coins: &[&str], height: u64) -> Batch<NodeDataOrderStatus> {
        let events: Vec<serde_json::Value> = coins
            .iter()
            .enumerate()
            .map(|(index, coin)| {
                serde_json::json!({
                    "time": "2026-11-25T20:00:00.000000000",
                    "user": "0x0000000000000000000000000000000000000001",
                    "hash": "0xabc",
                    "status": "open",
                    "order": {
                        "coin": coin, "side": "B", "limitPx": "1.0", "sz": "1.0", "oid": height * 1_000 + index as u64,
                        "timestamp": 0, "triggerCondition": "N/A", "isTrigger": false, "triggerPx": "0.0",
                        "children": [], "isPositionTpsl": false, "reduceOnly": false, "orderType": "Limit",
                        "origSz": "1.0", "tif": "Gtc", "cloid": null,
                    },
                })
            })
            .collect();
        serde_json::from_value(serde_json::json!({
            "local_time": "2026-11-25T20:00:00.000000000",
            "block_time": "2026-11-25T20:00:00.000000000",
            "block_number": height,
            "events": events,
        }))
        .unwrap()
    }

    fn diff_batch(coins: &[&str], height: u64) -> Batch<NodeDataOrderDiff> {
        let events: Vec<serde_json::Value> = coins
            .iter()
            .enumerate()
            .map(|(index, coin)| {
                serde_json::json!({
                    "user": "0x0000000000000000000000000000000000000001",
                    "oid": height * 1_000 + index as u64, "px": "1.0", "coin": coin,
                    "raw_book_diff": { "new": { "sz": "1.0" } },
                })
            })
            .collect();
        serde_json::from_value(serde_json::json!({
            "local_time": "2026-11-25T20:00:00.000000000",
            "block_time": "2026-11-25T20:00:00.000000000",
            "block_number": height,
            "events": events,
        }))
        .unwrap()
    }

    #[test]
    fn a_batch_is_divided_by_owner_and_every_group_sees_the_block() {
        let markets = MarketFilter::new(true, false, true, vec!["xyz".to_string()]);
        let tracked: Vec<&str> = COINS.iter().copied().filter(|coin| markets.includes(coin)).collect();
        for count in [2_usize, 3, 4] {
            let parts = split_for_shards(EventBatch::Orders(status_batch(&COINS, 77)), &markets, count);
            assert_eq!(parts.len(), count);
            let mut seen = Vec::new();
            for (index, part) in parts.into_iter().enumerate() {
                let Some(EventBatch::Orders(batch)) = part else { panic!("statuses stay statuses") };
                assert_eq!(batch.block_number(), 77, "an empty part still carries the block");
                for status in batch.events_ref() {
                    assert_eq!(ShardRoute::shard_of(&status.order.coin, count), index);
                    seen.push(status.order.coin.clone());
                }
            }
            seen.sort();
            let mut expected: Vec<String> = tracked.iter().map(|coin| (*coin).to_string()).collect();
            expected.sort();
            assert_eq!(seen, expected, "each tracked event lands in exactly one group, untracked ones in none");
        }
    }

    #[test]
    fn statuses_and_diffs_of_one_coin_reach_the_same_group() {
        // A status and its diff are paired inside one group's state.
        let markets = MarketFilter::all();
        let statuses = split_for_shards(EventBatch::Orders(status_batch(&COINS, 5)), &markets, 4);
        let diffs = split_for_shards(EventBatch::BookDiffs(diff_batch(&COINS, 5)), &markets, 4);
        for (status_part, diff_part) in statuses.into_iter().zip(diffs) {
            let (Some(EventBatch::Orders(statuses)), Some(EventBatch::BookDiffs(diffs))) = (status_part, diff_part) else {
                panic!("wrong variants")
            };
            let status_coins: Vec<&str> = statuses.events_ref().iter().map(|status| status.order.coin.as_str()).collect();
            let diff_coins: Vec<&str> = diffs.events_ref().iter().map(NodeDataOrderDiff::coin_str).collect();
            assert_eq!(status_coins, diff_coins);
        }
    }

    #[test]
    fn one_group_gets_the_whole_batch() {
        let parts = split_for_shards(EventBatch::Orders(status_batch(&COINS, 9)), &MarketFilter::all(), 1);
        let [Some(EventBatch::Orders(batch))] = parts.as_slice() else { panic!("one part") };
        assert_eq!(batch.events_len(), COINS.len());
    }

    #[test]
    fn the_group_universe_is_the_union_and_ready_needs_every_group() {
        let group = ShardGroup::new(2);
        assert!(!group.all_ready());
        let first: Arc<HashSet<String>> = Arc::new(["BTC".to_string()].into());
        assert_eq!(*group.publish(0, first), ["BTC".to_string()].into());
        assert!(!group.all_ready(), "one group of two is not the server");
        let merged = group.publish(1, Arc::new(["xyz:AMD".to_string()].into()));
        assert!(group.all_ready());
        assert_eq!(*merged, ["BTC".to_string(), "xyz:AMD".to_string()].into());
        // A group's later report replaces its earlier one.
        let merged = group.publish(0, Arc::new(["ETH".to_string()].into()));
        assert_eq!(*merged, ["ETH".to_string(), "xyz:AMD".to_string()].into());
        assert_eq!(*group.merged(), *merged);
    }

    fn part_coins(part: &SnapshotPart) -> (Vec<String>, Vec<String>) {
        let mut books: Vec<String> =
            part.snapshot.as_ref_for_test().keys().map(|coin| coin.as_str().to_string()).collect();
        books.sort();
        let mut untriggered: Vec<String> = part.untriggered.iter().map(|order| order.coin.as_str().to_string()).collect();
        untriggered.sort();
        (books, untriggered)
    }

    #[tokio::test(start_paused = true)]
    async fn groups_share_one_dump_and_each_takes_only_its_coins() {
        let dumps = Arc::new(AtomicUsize::new(0));
        let broker = {
            let dumps = Arc::clone(&dumps);
            SnapshotBroker::fixed(3, move || {
                dumps.fetch_add(1, Ordering::SeqCst);
                Ok(dump_of(&COINS))
            })
        };
        let markets = MarketFilter::new(true, false, true, vec!["xyz".to_string()]);
        let caching_since = Instant::now();

        let mut all_books = Vec::new();
        for index in 0..3 {
            let group_markets = markets.for_shard(ShardRoute::new(index, 3));
            let part = broker.fetch(caching_since, &group_markets).await.unwrap();
            assert_eq!(part.height, 42);
            assert!(part.dump_had_untriggered, "the flag describes the whole dump, not the group's share");
            let (books, untriggered) = part_coins(&part);
            assert_eq!(books, untriggered);
            assert!(books.iter().all(|coin| group_markets.includes(coin)));
            all_books.extend(books);
        }
        assert_eq!(dumps.load(Ordering::SeqCst), 1, "three groups that began caching before the dump share it");
        all_books.sort();
        let mut expected: Vec<String> =
            COINS.iter().filter(|coin| markets.includes(coin)).map(|coin| (*coin).to_string()).collect();
        expected.sort();
        assert_eq!(all_books, expected);
    }

    #[tokio::test(start_paused = true)]
    async fn a_group_that_began_caching_after_the_dump_gets_a_new_one() {
        let dumps = Arc::new(AtomicUsize::new(0));
        let broker = {
            let dumps = Arc::clone(&dumps);
            SnapshotBroker::fixed(1, move || {
                dumps.fetch_add(1, Ordering::SeqCst);
                Ok(dump_of(&COINS))
            })
        };
        let markets = MarketFilter::all();
        broker.fetch(Instant::now(), &markets).await.unwrap();
        let first_done = Instant::now();
        tokio::time::sleep(Duration::from_millis(5)).await;

        // Its cache starts after the first dump, so that dump may hold events it never cached.
        let part = broker.fetch(Instant::now(), &markets).await.unwrap();
        assert_eq!(dumps.load(Ordering::SeqCst), 2);
        assert_eq!(part_coins(&part).0.len(), COINS.len(), "a new dump is whole again");
        assert!(Instant::now() - first_done >= DUMP_MIN_GAP, "dumps are spaced out, whoever asks");
    }

    #[tokio::test(start_paused = true)]
    async fn groups_that_begin_caching_moments_apart_share_the_dump() {
        // Group threads ask within milliseconds of each other; neither should
        // have to wait for a dump of its own.
        let dumps = Arc::new(AtomicUsize::new(0));
        let broker = {
            let dumps = Arc::clone(&dumps);
            SnapshotBroker::fixed(2, move || {
                dumps.fetch_add(1, Ordering::SeqCst);
                Ok(dump_of(&COINS))
            })
        };
        let first = {
            let broker = Arc::clone(&broker);
            let markets = MarketFilter::all().for_shard(ShardRoute::new(0, 2));
            let caching_since = Instant::now();
            tokio::spawn(async move { broker.fetch(caching_since, &markets).await.map(|part| part.height) })
        };
        tokio::time::sleep(DUMP_SETTLE / 2).await;
        let second = MarketFilter::all().for_shard(ShardRoute::new(1, 2));
        broker.fetch(Instant::now(), &second).await.unwrap();
        assert_eq!(first.await.unwrap().unwrap(), 42);
        assert_eq!(dumps.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn nothing_is_kept_once_every_group_has_its_share() {
        // Coins of markets nobody tracks would otherwise sit in memory until the timer.
        let broker = SnapshotBroker::fixed(2, || Ok(dump_of(&COINS)));
        let markets = MarketFilter::new(true, false, false, Vec::new());
        let caching_since = Instant::now();
        for index in 0..2 {
            broker.fetch(caching_since, &markets.for_shard(ShardRoute::new(index, 2))).await.unwrap();
            let latest = broker.latest.lock().await;
            let rest = latest.dump.as_ref().unwrap().rest.lock().unwrap();
            let left = rest.snapshot.as_ref_for_test().len() + rest.untriggered.len();
            assert_eq!(left == 0, index == 1, "after group {index}");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_dump_is_reported_and_not_kept() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let broker = {
            let attempts = Arc::clone(&attempts);
            SnapshotBroker::fixed(1, move || {
                if attempts.fetch_add(1, Ordering::SeqCst) == 0 { Err("node busy".into()) } else { Ok(dump_of(&COINS)) }
            })
        };
        let caching_since = Instant::now();
        assert!(broker.fetch(caching_since, &MarketFilter::all()).await.is_err());
        assert!(broker.fetch(caching_since, &MarketFilter::all()).await.is_ok());
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    /// Three groups running their real loops against one fixed dump.
    async fn start_groups(
        count: usize,
        dumps: &Arc<AtomicUsize>,
    ) -> (BookShards, Vec<tokio::sync::mpsc::Sender<Vec<ShardEvent>>>) {
        let (tx, _rx) = tokio::sync::broadcast::channel(1024);
        let books = BookShards::new(count, Some(tx), MarketFilter::all(), ActiveL2Params::new(), |_| {});
        let broker = {
            let dumps = Arc::clone(dumps);
            SnapshotBroker::fixed(count, move || {
                dumps.fetch_add(1, Ordering::SeqCst);
                Ok(dump_of(&COINS))
            })
        };
        let mut senders = Vec::new();
        for listener in books.listeners() {
            let (events_tx, events_rx) = tokio::sync::mpsc::channel(64);
            senders.push(events_tx);
            let (listener, broker, group) = (Arc::clone(listener), Arc::clone(&broker), Arc::clone(books.group()));
            tokio::spawn(async move { drop(super::super::run_shard(listener, events_rx, broker, group).await) });
        }
        // Each loop asks for its first snapshot a few seconds after it starts.
        for _ in 0..300 {
            if books.is_ready().await {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        (books, senders)
    }

    /// Hand a batch to the groups the way the reader does.
    async fn deliver(
        books: &BookShards,
        senders: &[tokio::sync::mpsc::Sender<Vec<ShardEvent>>],
        height: u64,
        batch: EventBatch,
        source: EventSource,
    ) {
        for (sender, part) in senders.iter().zip(split_for_shards(batch, books.markets(), books.count())) {
            if let Some(part) = part {
                sender.send(vec![ShardEvent::Apply(height, part, source)]).await.unwrap();
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    #[tokio::test(start_paused = true)]
    async fn groups_start_from_one_dump_and_hold_only_their_own_coins() {
        let dumps = Arc::new(AtomicUsize::new(0));
        let (books, _senders) = start_groups(3, &dumps).await;

        assert!(books.is_ready().await);
        assert!(books.group().all_ready());
        assert_eq!(dumps.load(Ordering::SeqCst), 1, "the groups start together, so one dump serves all three");
        let expected: HashSet<String> = COINS.iter().map(|coin| (*coin).to_string()).collect();
        assert_eq!(*books.universe().await, expected);
        assert_eq!(*books.group().merged(), expected);

        for coin in COINS {
            for (index, listener) in books.listeners().iter().enumerate() {
                let held = listener.lock().await.compute_snapshot_for_coin(&Coin::new(coin), Default::default()).is_some();
                assert_eq!(held, index == ShardRoute::shard_of(coin, 3), "{coin} in group {index}");
            }
        }
        // Untriggered orders of all groups come back as one list.
        let (_, _, untriggered) = books.untriggered_snapshot(None).await.unwrap();
        assert_eq!(untriggered.len(), COINS.len());
        let (_, _, one_coin) = books.untriggered_snapshot(Some(&Coin::new("xyz:AMD"))).await.unwrap();
        assert_eq!(one_coin.len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn an_event_changes_its_owner_group_and_every_group_follows_the_block() {
        let dumps = Arc::new(AtomicUsize::new(0));
        let (books, senders) = start_groups(3, &dumps).await;
        assert!(books.is_ready().await);

        // A new BTC order: its status and its diff both go to BTC's group.
        deliver(&books, &senders, 50, EventBatch::Orders(status_batch(&["BTC"], 50)), EventSource::OrderStatuses).await;
        deliver(&books, &senders, 50, EventBatch::BookDiffs(diff_batch(&["BTC"], 50)), EventSource::OrderDiffs).await;

        let owner = ShardRoute::shard_of("BTC", 3);
        for (index, listener) in books.listeners().iter().enumerate() {
            let guard = listener.lock().await;
            let (_, height, _) = guard.compute_untriggered_snapshot(None).unwrap();
            assert_eq!(height, 50, "group {index} saw the block although it may hold no event of it");
            let bids = guard
                .compute_snapshot_for_coin(&Coin::new("BTC"), Default::default())
                .map(|(_, _, book)| book.as_ref()[0].len());
            let expected = (index == owner).then_some(2);
            assert_eq!(bids, expected, "the dump's order plus the new one, in BTC's group only");
        }
        assert_eq!(dumps.load(Ordering::SeqCst), 1, "applying events needs no further dump");

        // One group moves ahead of the others. A list over all groups is stamped
        // with the height every group has reached, so resuming from it misses nothing.
        books.listeners()[owner].lock().await.apply_event_batch(
            60,
            EventBatch::Orders(status_batch(&[], 60)),
            EventSource::OrderStatuses,
        );
        let (_, height, _) = books.untriggered_snapshot(None).await.unwrap();
        assert_eq!(height, 50);
    }

    #[tokio::test(start_paused = true)]
    async fn a_listener_keeps_the_start_of_its_cache_until_a_new_one_begins() {
        // At startup the cache runs from construction, so asking for the first
        // snapshot later must not move the stamp: that is what lets all groups share it.
        let books = BookShards::new(1, None, MarketFilter::all(), ActiveL2Params::new(), |_| {});
        let listener = &books.listeners()[0];
        let built = listener.lock().await.caching_since;
        tokio::time::sleep(Duration::from_secs(5)).await;
        listener.lock().await.begin_caching();
        assert_eq!(listener.lock().await.caching_since, built);

        // After an install the cache is gone; the next one starts a new stamp.
        listener.lock().await.init_from_snapshot(Snapshots::new(HashMap::new()), 10);
        tokio::time::sleep(Duration::from_secs(5)).await;
        listener.lock().await.begin_caching();
        assert!(listener.lock().await.caching_since > built);
    }

    #[tokio::test(start_paused = true)]
    async fn the_unclaimed_part_of_a_dump_is_dropped_after_a_while() {
        let broker = SnapshotBroker::fixed(2, || Ok(dump_of(&COINS)));
        let caching_since = Instant::now();
        let first = MarketFilter::all().for_shard(ShardRoute::new(0, 2));
        broker.fetch(caching_since, &first).await.unwrap();
        assert!(broker.latest.lock().await.dump.is_some());

        tokio::time::sleep(DUMP_KEEP + Duration::from_secs(1)).await;
        assert!(broker.latest.lock().await.dump.is_none());
    }
}
