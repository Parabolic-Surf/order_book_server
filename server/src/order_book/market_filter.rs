//! Which markets a server instance tracks.

use std::fmt;

use super::Coin;

/// One group of coins out of `count`. Each group's books are owned by one
/// thread, so the work of applying events spreads over several cores. A coin
/// always maps to the same group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ShardRoute {
    index: usize,
    count: usize,
}

impl ShardRoute {
    /// The only group: owns every coin.
    pub(crate) const SINGLE: Self = Self { index: 0, count: 1 };

    pub(crate) fn new(index: usize, count: usize) -> Self {
        assert!(count > 0 && index < count, "group {index} of {count}");
        Self { index, count }
    }

    pub(crate) const fn index(self) -> usize {
        self.index
    }

    pub(crate) const fn count(self) -> usize {
        self.count
    }

    /// The group that owns `coin` when coins are split into `count` groups.
    pub(crate) fn shard_of(coin: &str, count: usize) -> usize {
        if count <= 1 {
            return 0;
        }
        // FNV-1a: stable across runs and builds, unlike the std hasher.
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        for byte in coin.bytes() {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        // The low bits of FNV are weak for short keys; fold the high half in.
        ((hash ^ (hash >> 32)) % count as u64) as usize
    }

    pub(crate) fn owns(self, coin: &str) -> bool {
        self.count == 1 || Self::shard_of(coin, self.count) == self.index
    }
}

/// Selects the markets a server instance tracks. Events for every other market
/// are skipped before they reach any book state, so several instances can read
/// one node and each carry part of the load.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketFilter {
    perps: bool,
    spot: bool,
    hip3: bool,
    /// HIP-3 dex names to keep; empty keeps every dex.
    hip3_dexes: Vec<String>,
    /// Nothing is filtered, so the hot path can skip classifying the coin.
    everything: bool,
    /// The group of coins this copy of the filter is for.
    shard: ShardRoute,
}

impl MarketFilter {
    /// `hip3_dexes` narrows HIP-3 to the named dexes; empty keeps every dex.
    #[must_use]
    pub const fn new(perps: bool, spot: bool, hip3: bool, hip3_dexes: Vec<String>) -> Self {
        let everything = perps && spot && hip3 && hip3_dexes.is_empty();
        Self { perps, spot, hip3, hip3_dexes, everything, shard: ShardRoute::SINGLE }
    }

    /// Every market: perps, spot and all HIP-3 dexes.
    #[must_use]
    pub fn all() -> Self {
        Self::new(true, true, true, Vec::new())
    }

    /// The same markets, narrowed to the coins one group owns.
    pub(crate) fn for_shard(&self, shard: ShardRoute) -> Self {
        Self { shard, ..self.clone() }
    }

    pub(crate) const fn shard(&self) -> ShardRoute {
        self.shard
    }

    /// True when events for `coin` should be applied and served.
    #[must_use]
    pub fn includes(&self, coin: &str) -> bool {
        self.shard.owns(coin) && self.includes_market(coin)
    }

    /// The market rule alone, for any group.
    pub(crate) fn includes_market(&self, coin: &str) -> bool {
        if self.everything {
            return true;
        }
        if Coin::str_is_spot(coin) {
            return self.spot;
        }
        match coin.split_once(':') {
            Some((dex, _)) => self.hip3 && (self.hip3_dexes.is_empty() || self.hip3_dexes.iter().any(|d| d == dex)),
            None => self.perps,
        }
    }

    pub(crate) fn includes_coin(&self, coin: &Coin) -> bool {
        self.includes(coin.as_str())
    }

    /// True when spot markets are tracked.
    #[must_use]
    pub const fn includes_spot(&self) -> bool {
        self.spot
    }

    /// True when no market is filtered out.
    #[must_use]
    pub const fn tracks_everything(&self) -> bool {
        self.everything
    }

    /// True when this filter drops nothing at all: every market, one group.
    pub(crate) const fn passes_everything(&self) -> bool {
        self.everything && self.shard.count == 1
    }
}

impl Default for MarketFilter {
    fn default() -> Self {
        Self::all()
    }
}

impl fmt::Display for MarketFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.everything {
            return f.write_str("all");
        }
        let mut parts = Vec::new();
        if self.perps {
            parts.push("perps".to_string());
        }
        if self.spot {
            parts.push("spot".to_string());
        }
        if self.hip3 {
            if self.hip3_dexes.is_empty() {
                parts.push("hip3".to_string());
            } else {
                parts.push(format!("hip3({})", self.hip3_dexes.join(",")));
            }
        }
        if parts.is_empty() {
            return f.write_str("none");
        }
        f.write_str(&parts.join("+"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_includes_every_market() {
        let filter = MarketFilter::all();
        for coin in ["BTC", "kPEPE", "@107", "PURR/USDC", "xyz:AMD", "io:GOLD"] {
            assert!(filter.includes(coin), "{coin}");
        }
        assert!(filter.tracks_everything());
        assert!(!MarketFilter::new(true, true, true, vec!["xyz".to_string()]).tracks_everything());
        assert!(!MarketFilter::new(true, false, true, Vec::new()).tracks_everything());
        assert_eq!(filter.to_string(), "all");
    }

    #[test]
    fn perps_only_skips_spot_and_hip3() {
        let filter = MarketFilter::new(true, false, false, Vec::new());
        assert!(filter.includes("BTC"));
        assert!(filter.includes("kPEPE"));
        assert!(!filter.includes("@107"));
        assert!(!filter.includes("PURR/USDC"));
        assert!(!filter.includes("xyz:AMD"));
        assert!(!filter.includes_spot());
        assert_eq!(filter.to_string(), "perps");
    }

    #[test]
    fn hip3_only_skips_perps_and_spot() {
        let filter = MarketFilter::new(false, false, true, Vec::new());
        assert!(filter.includes("xyz:AMD"));
        assert!(filter.includes("io:GOLD"));
        assert!(!filter.includes("BTC"));
        assert!(!filter.includes("@107"));
        assert_eq!(filter.to_string(), "hip3");
    }

    #[test]
    fn dex_list_narrows_hip3() {
        let filter = MarketFilter::new(false, false, true, vec!["xyz".to_string()]);
        assert!(filter.includes("xyz:AMD"));
        assert!(!filter.includes("io:GOLD"));
        // A dex name must match whole, not as a prefix.
        assert!(!filter.includes("xyzz:AMD"));
        assert!(!filter.includes("BTC"));
        assert_eq!(filter.to_string(), "hip3(xyz)");
    }

    #[test]
    fn dex_list_does_not_affect_perps() {
        let filter = MarketFilter::new(true, false, true, vec!["xyz".to_string()]);
        assert!(filter.includes("BTC"));
        assert!(filter.includes("xyz:AMD"));
        assert!(!filter.includes("io:GOLD"));
        assert!(!filter.includes("@107"));
        assert_eq!(filter.to_string(), "perps+hip3(xyz)");
    }

    #[test]
    fn spot_is_classified_before_the_dex_separator() {
        // Spot names never contain ':'; a spot filter must not admit HIP-3 coins.
        let filter = MarketFilter::new(false, true, false, Vec::new());
        assert!(filter.includes("@107"));
        assert!(filter.includes("PURR/USDC"));
        assert!(!filter.includes("xyz:AMD"));
        assert!(!filter.includes("BTC"));
        assert!(filter.includes_spot());
    }

    const COINS: [&str; 12] =
        ["BTC", "ETH", "SOL", "HYPE", "kPEPE", "DOGE", "xyz:AMD", "xyz:NVDA", "xyz:GOLD", "io:GOLD", "@107", "PURR/USDC"];

    #[test]
    fn every_coin_belongs_to_exactly_one_group() {
        for count in 1..=8 {
            for coin in COINS {
                let owners = (0..count).filter(|&index| ShardRoute::new(index, count).owns(coin)).count();
                assert_eq!(owners, 1, "{coin} with {count} groups");
                assert!(ShardRoute::shard_of(coin, count) < count);
            }
        }
    }

    #[test]
    fn group_assignment_is_fixed() {
        // The dispatcher and every group must agree across restarts and builds,
        // so the mapping is pinned here; a change to the hash shows up as a failure.
        let got: Vec<usize> = ["BTC", "ETH", "xyz:AMD"].iter().map(|coin| ShardRoute::shard_of(coin, 4)).collect();
        assert_eq!(got, vec![1, 1, 0]);
        assert_eq!(ShardRoute::shard_of("BTC", 1), 0);
        assert_eq!(ShardRoute::shard_of("BTC", 0), 0);
    }

    #[test]
    fn groups_split_coins_roughly_evenly() {
        let names: Vec<String> = (0..2000).map(|i| format!("COIN{i}")).collect();
        for count in [2_usize, 3, 4, 8] {
            let mut sizes = vec![0_usize; count];
            for name in &names {
                sizes[ShardRoute::shard_of(name, count)] += 1;
            }
            let expected = names.len() / count;
            for size in sizes {
                assert!(size > expected * 8 / 10 && size < expected * 12 / 10, "{size} of {expected} with {count} groups");
            }
        }
    }

    #[test]
    fn a_group_filter_keeps_the_market_rule_and_its_own_coins_only() {
        let markets = MarketFilter::new(true, false, true, vec!["xyz".to_string()]);
        let groups: Vec<MarketFilter> = (0..3).map(|index| markets.for_shard(ShardRoute::new(index, 3))).collect();
        for coin in COINS {
            let owners = groups.iter().filter(|group| group.includes(coin)).count();
            assert_eq!(owners, usize::from(markets.includes(coin)), "{coin}");
            assert!(groups.iter().all(|group| group.includes_market(coin) == markets.includes(coin)));
        }
        assert_eq!(groups[1].shard(), ShardRoute::new(1, 3));
        assert_eq!(markets.shard(), ShardRoute::SINGLE);
    }

    #[test]
    fn only_an_unfiltered_single_group_passes_everything() {
        assert!(MarketFilter::all().passes_everything());
        assert!(!MarketFilter::all().for_shard(ShardRoute::new(0, 2)).passes_everything());
        assert!(MarketFilter::all().for_shard(ShardRoute::new(0, 2)).tracks_everything());
        assert!(!MarketFilter::new(true, false, true, Vec::new()).passes_everything());
    }
}
