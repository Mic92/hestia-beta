# A CI trace from git's history

`walk.sh <repo> <base> <tip> <out dir>` builds every commit in
`<base>..<tip>` of a git.git checkout, incrementally and through ccache,
and writes `trace.tsv` (commit, output path, hash of its inputs, hash of
its bytes, size) and `commits.tsv` (commit, parents, commit time).
`keys.py` computes the lines for one built commit from the compiler's
dependency files.

`examples/node_cache_sim.rs` replays the two files. The trace behind
`docs/cache-node.md` is the 767 commits of the 200 first-parent commits
before 20ceead5c3 (May 2024), built with gcc `-g -O2` and no optional
dependencies; it took about 75 minutes of build time on four cores.
