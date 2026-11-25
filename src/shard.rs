//! Shards of a root, for stores served by more than one cache node.
//!
//! A sharded store keeps the paths of a root under 32 roots, one per
//! first character of the store path hash: `<root>.<character>`. A
//! proxy in front of the nodes sends everything for one shard to one
//! node, so a lookup arrives where the commit went and the nodes need
//! not talk to each other. A node serves a shard from the first request
//! for it, so the node that stands in for a dead one loads what that
//! one published. Which node has which shard is the proxy's business:
//! no node knows the others.
//!
//! The first character can be read off a narinfo URL (`/<hash>.narinfo`)
//! and off the `hash` parameter of a NAR URL. Requests that carry the
//! hash only in the body name the shard in [`HEADER`].

use crate::manifest::PathHash;

/// Names the shard of a plan, pack or commit request to the proxy.
pub const HEADER: &str = "x-hestia-shard";

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Shard(char);

impl Shard {
    pub fn of(hash: &PathHash) -> Shard {
        let text = hash.to_string();
        Shard(text.chars().next().expect("a path hash is not empty"))
    }

    /// The root that holds this shard of `root`.
    pub fn root(self, root: &str) -> String {
        format!("{root}.{}", self.0)
    }
}

impl std::fmt::Display for Shard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_shard_is_the_first_character_of_the_hash() {
        let hash: PathHash = "7rjj86a15146cq1d3qy068lml7n7ykzm".parse().unwrap();
        assert_eq!(Shard::of(&hash).to_string(), "7");
        assert_eq!(
            Shard::of(&hash).root("main-x86_64-linux"),
            "main-x86_64-linux.7"
        );
    }
}
