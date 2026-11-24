"""Print one line per build output of a git checkout: commit, path, key of
its inputs, hash of its bytes, size. Usage: keys.py <repo> <commit>"""
import hashlib, os, subprocess, sys

repo, commit = sys.argv[1], sys.argv[2]
blobs = {}
for line in subprocess.run(["git", "-C", repo, "ls-tree", "-r", commit],
                           capture_output=True, text=True, check=True).stdout.splitlines():
    meta, path = line.split("\t", 1)
    blobs[path] = meta.split()[2]

def sha(path):
    h = hashlib.sha1()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()

rows, keys = [], []
for dirpath, dirs, files in os.walk(repo):
    dirs[:] = [d for d in dirs if d not in (".git", ".depend")]
    for name in files:
        if not name.endswith(".o"):
            continue
        obj = os.path.join(dirpath, name)
        rel = os.path.relpath(obj, repo)
        dep = os.path.join(dirpath, ".depend", name + ".d")
        if not os.path.exists(dep):
            continue
        rule = open(dep).read().replace("\\\n", " ").split("\n", 1)[0]
        deps = rule.split(":", 1)[1].split()
        if not deps or os.path.normpath(deps[0]) not in blobs:
            continue  # left over from a source file this commit lacks
        inputs = []
        for d in deps:
            if os.path.isabs(d):
                continue
            p = os.path.normpath(d)
            if p in blobs:
                inputs.append(f"{p} {blobs[p]}")
            elif os.path.exists(os.path.join(repo, p)):
                inputs.append(f"{p} {sha(os.path.join(repo, p))}")
        key = hashlib.sha1("\n".join(sorted(inputs)).encode()).hexdigest()
        keys.append(key)
        rows.append((rel, key, sha(obj), os.path.getsize(obj)))
link = hashlib.sha1("\n".join(sorted(keys)).encode()).hexdigest()
for rel in ("git", "libgit.a", "xdiff/lib.a", "reftable/libreftable.a"):
    p = os.path.join(repo, rel)
    if os.path.exists(p):
        key = hashlib.sha1(f"{rel} {link}".encode()).hexdigest()
        rows.append((rel, key, sha(p), os.path.getsize(p)))
for row in sorted(rows):
    print(commit, *row, sep="\t")
