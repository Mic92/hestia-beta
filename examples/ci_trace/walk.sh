#!/bin/sh
# Build every commit in <base>..<tip> of a git checkout, incrementally and
# through ccache, and record the inputs key, bytes hash and size of each output.
# Usage: walk.sh <repo> <base> <tip> <out dir>
R=$1; BASE=$2; TIP=$3; OUT=$4
T=$(dirname "$0")
export CCACHE_DIR="$OUT/ccache" CCACHE_MAXSIZE=30G
mk() { make -C "$R" -j4 CC="ccache gcc" CFLAGS="-g -O2 -fdebug-prefix-map=$R=/build" \
  NO_OPENSSL=1 NO_CURL=1 NO_EXPAT=1 NO_GETTEXT=1 NO_TCLTK=1 NO_PERL=1 NO_PYTHON=1 \
  COMPUTE_HEADER_DEPENDENCIES=yes git > "$OUT/make.log" 2>&1; }
git -C "$R" log --format='%H %P%x09%ct' "$BASE..$TIP" > "$OUT/commits.tsv"
n=0
for c in $(git -C "$R" rev-list --topo-order --reverse "$BASE..$TIP"); do
  n=$((n + 1))
  grep -q "^$c" "$OUT/done" 2>/dev/null && continue
  s=$(date +%s)
  git -C "$R" checkout -q -f "$c" || exit 1
  if ! mk; then
    find "$R" -name '*.o' -delete; find "$R" -name '*.a' -delete
    find "$R" -name .depend -prune -exec rm -rf {} +
    mk || { echo "$c failed" >> "$OUT/progress"; continue; }
  fi
  python3 -I "$T/keys.py" "$R" "$c" >> "$OUT/trace.tsv" || exit 1
  echo "$c" >> "$OUT/done"
  echo "$n $c $(( $(date +%s) - s )) s" >> "$OUT/progress"
done
echo finished >> "$OUT/progress"
