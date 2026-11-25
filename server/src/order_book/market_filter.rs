//! Which markets a server instance tracks.

use std::fmt;

use super::Coin;

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
}

impl MarketFilter {
    /// `hip3_dexes` narrows HIP-3 to the named dexes; empty keeps every dex.
    #[must_use]
    pub const fn new(perps: bool, spot: bool, hip3: bool, hip3_dexes: Vec<String>) -> Self {
        let everything = perps && spot && hip3 && hip3_dexes.is_empty();
        Self { perps, spot, hip3, hip3_dexes, everything }
    }

    /// Every market: perps, spot and all HIP-3 dexes.
    #[must_use]
    pub fn all() -> Self {
        Self::new(true, true, true, Vec::new())
    }

    /// True when events for `coin` should be applied and served.
    #[must_use]
    pub fn includes(&self, coin: &str) -> bool {
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
}
