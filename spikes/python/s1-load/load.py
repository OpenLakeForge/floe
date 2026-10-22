# /// script
# requires-python = ">=3.10"
# dependencies = ["pyiceberg==0.12.0", "pyarrow==25.0.1", "polars==2.0.0"]
# ///
"""S1 loader (#597): CSV -> contract cast -> Iceberg, one catalog commit per run, run-id guard.

Write paths (--path):
  chunked    one Transaction; delete(AlwaysTrue()) for overwrite, then tx.append(chunk) per chunk
  stream     one Transaction; delete(AlwaysTrue()) for overwrite, then one tx.append(RecordBatchReader)
  single     one Transaction, one snapshot producer (overwrite or fast_append) fed every chunk
  add_files  stage each chunk as Parquet under <table>/data/staging/, then delete + add_files

--pause-at stops the process at a crash point (writes --pause-file, then sleeps) so crash.py can
kill -9 it there: read (first chunk read, nothing written), pre-commit (after --pause-after
chunks, before the catalog commit), post-commit (committed, state marker not yet written).
"""

import argparse
import hashlib
import itertools
import json
import os
import resource
import sys
import threading
import time
import uuid

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.expressions import AlwaysTrue
from pyiceberg.io.pyarrow import _dataframe_to_data_files

from common import CASTS, COLUMNS, CONTRACT, NAMESPACE, catalog

RUN_ID, INPUT_HASH = "floe.run_id", "floe.input_hash"


class Memory:
    """Peak anonymous RSS, sampled. VmHWM also counts the CSV pages Polars memory-maps, which the
    kernel can reclaim, so both are reported."""

    def __init__(self) -> None:
        self.peak_anon_mb = 0
        threading.Thread(target=self._run, daemon=True).start()

    @staticmethod
    def status(key: str) -> int:
        with open("/proc/self/status") as f:
            return next(int(line.split()[1]) // 1024 for line in f if line.startswith(key + ":"))

    def _run(self) -> None:
        while True:
            self.peak_anon_mb = max(self.peak_anon_mb, self.status("RssAnon"))
            time.sleep(0.1)


def input_hash(paths: list[str]) -> str:
    # ponytail: fingerprints name + size + mtime, like a cheap file claim; hashing content would
    # cost a full extra read of the input. Upgrade: hash content, or reuse Floe's claim identity.
    h = hashlib.sha256()
    for p in sorted(paths):
        st = os.stat(p)
        h.update(f"{os.path.basename(p)}|{st.st_size}|{st.st_mtime_ns}\n".encode())
    return h.hexdigest()


def find_run(table, run_id: str):
    for snap in table.snapshots():
        if snap.summary is not None and snap.summary.additional_properties.get(RUN_ID) == run_id:
            return snap
    return None


def pause(args, point: str) -> None:
    if args.pause_at == point:
        with open(args.pause_file, "w") as f:
            f.write(str(os.getpid()))
        print(f"paused at {point}", flush=True)
        time.sleep(3600)


def read_chunks(args, mem: Memory, log: list):
    lf = pl.scan_csv(args.csv, schema={c: pl.String for c in COLUMNS}).select(CASTS)
    for i, df in enumerate(lf.collect_batches(chunk_size=args.chunk_rows)):
        chunk = df.to_arrow().cast(CONTRACT)
        log.append({"chunk": i, "rows": chunk.num_rows, "rss_anon_mb": Memory.status("RssAnon"), "rss_mb": Memory.status("VmRSS")})
        if i == 0:
            pause(args, "read")
        if i == args.pause_after:
            pause(args, "pre-commit")
        yield chunk


def write(args, table, tx, props, chunks) -> None:
    overwrite = args.mode == "overwrite"
    if args.path == "chunked":
        if overwrite:
            tx.delete(AlwaysTrue(), snapshot_properties=props)
        for chunk in chunks:
            tx.append(chunk, snapshot_properties=props)
    elif args.path == "stream":
        if overwrite:
            tx.delete(AlwaysTrue(), snapshot_properties=props)
        batches = (b for chunk in chunks for b in chunk.to_batches())
        tx.append(pa.RecordBatchReader.from_batches(CONTRACT, batches), snapshot_properties=props)
    elif args.path == "single":
        update = tx.update_snapshot(snapshot_properties=props)
        with update.overwrite() if overwrite else update.fast_append() as snap:
            if overwrite:
                for task in tx._scan().plan_files():
                    snap.delete_data_file(task.file)
            counter = itertools.count(0)  # shared, so file names stay unique across chunks
            for chunk in chunks:
                for data_file in _dataframe_to_data_files(tx.table_metadata, chunk, table.io, snap.commit_uuid, counter):
                    snap.append_data_file(data_file)
    elif args.path == "add_files":
        staging = f"{table.location()}/data/staging/{args.run_id}-{uuid.uuid4().hex[:8]}"
        paths = []
        for i, chunk in enumerate(chunks):
            path = f"{staging}/chunk-{i:05d}.parquet"
            with table.io.new_output(path).create() as out:
                pq.write_table(chunk, out)
            paths.append(path)
        if overwrite:
            tx.delete(AlwaysTrue(), snapshot_properties=props)
        tx.add_files(paths, snapshot_properties=props)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--csv", required=True)
    p.add_argument("--table", required=True)
    p.add_argument("--mode", choices=["overwrite", "append"], required=True)
    p.add_argument("--path", choices=["chunked", "stream", "single", "add_files"], default="chunked")
    p.add_argument("--run-id", required=True)
    p.add_argument("--chunk-rows", type=int, default=1_000_000)
    p.add_argument("--state-dir", required=True, help="where the 'state promoted' marker is written")
    p.add_argument("--no-guard", action="store_true", help="disable the run-id check (negative test)")
    p.add_argument("--pause-at", choices=["read", "pre-commit", "post-commit"])
    p.add_argument("--pause-after", type=int, default=3, help="chunks written before the pre-commit pause")
    p.add_argument("--pause-file", default="/tmp/s1.paused")
    p.add_argument("--stats-out")
    args = p.parse_args()

    mem, t0, log = Memory(), time.time(), []
    cat = catalog()
    ident = f"{NAMESPACE}.{args.table}"
    table = cat.create_table_if_not_exists(ident, schema=CONTRACT)
    commits = 0
    real_commit = cat.commit_table

    def counting_commit(*a, **kw):
        nonlocal commits
        commits += 1
        return real_commit(*a, **kw)

    cat.commit_table = counting_commit
    marker = os.path.join(args.state_dir, f"{args.table}__{args.run_id}.promoted")
    snaps_before = {s.snapshot_id for s in table.snapshots()}
    log_before = len(table.metadata.metadata_log)
    stats = {"table": args.table, "mode": args.mode, "path": args.path, "run_id": args.run_id, "chunk_rows": args.chunk_rows}

    if os.path.exists(marker):
        stats["outcome"] = "state already promoted, nothing to do"
    else:
        props = {RUN_ID: args.run_id, INPUT_HASH: input_hash([args.csv])}
        done = None if args.no_guard else find_run(table, args.run_id)
        if done is not None:
            if done.summary.additional_properties.get(INPUT_HASH) != props[INPUT_HASH]:
                print(f"run {args.run_id} already committed with different inputs", file=sys.stderr)
                return 2
            stats["outcome"] = f"run id found in snapshot {done.snapshot_id}: write skipped"
        else:
            tx = table.transaction()
            write(args, table, tx, props, read_chunks(args, mem, log))
            tx.commit_transaction()
            stats["outcome"] = "written"
            pause(args, "post-commit")
        os.makedirs(args.state_dir, exist_ok=True)
        with open(marker, "w") as f:
            f.write(str(table.refresh().metadata.current_snapshot_id))

    table.refresh()
    snaps_after = [s for s in table.snapshots() if s.snapshot_id not in snaps_before]
    stats |= {
        "rows_read": sum(c["rows"] for c in log),
        "chunks": len(log),
        "client_commits": commits,
        "metadata_versions_added": len(table.metadata.metadata_log) - log_before,
        "snapshots_added": len(snaps_after),
        "snapshot_operations": [s.summary.operation.value for s in snaps_after],
        "wall_s": round(time.time() - t0, 1),
        "peak_rss_mb": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024,
        "peak_rss_anon_mb": mem.peak_anon_mb,
        "rss_anon_mb_by_chunk": [c["rss_anon_mb"] for c in log],
    }
    # cgroup v1 / v2 peak (includes page cache); only meaningful inside the container.
    for name in ("memory.max_usage_in_bytes", "memory.peak") if os.path.exists("/.dockerenv") else ():
        for root in ("/sys/fs/cgroup/memory", "/sys/fs/cgroup"):
            path = os.path.join(root, name)
            if os.path.exists(path):
                stats["cgroup_peak_mb"] = int(open(path).read()) // 2**20
    print(json.dumps({k: v for k, v in stats.items() if k != "rss_anon_mb_by_chunk"}), flush=True)
    if args.stats_out:
        with open(args.stats_out, "a") as f:
            f.write(json.dumps(stats) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
