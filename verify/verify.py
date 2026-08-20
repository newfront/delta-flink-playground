#!/usr/bin/env python3
"""Read-back verification for the delta-flink playground demos.

The delta-flink sink declares the `v2Checkpoint` reader feature, which the
delta-rs (deltalake) reader rejects. So instead of a full Delta reader we parse
the `_delta_log` ourselves to find the active data files, read those Parquet
files directly with pyarrow, and assert row/key counts. Deletion-vector presence
(proof of merge-on-read) is checked by scanning the commit JSON for add/remove
actions that carry a `deletionVector`.

Usage:
    python verify.py                # default local demos: 01, 02
    python verify.py 02_upsert_pk_mor
    python verify.py all            # every demo (S3 demos need RustFS reachable)

Local tables live under ../data/<demo> (bind-mounted from the Flink containers).
S3 demos read from the RustFS store the neighbouring unitycatalog-playground
publishes on the host (default http://localhost:9000).
"""
from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"

# Bundled MinIO is published on the host at :9002 (the Flink containers reach it
# in-network at minio:9000).
S3_ENDPOINT = os.environ.get("VERIFY_S3_ENDPOINT", "http://localhost:9002")
RUSTFS_KEY = os.environ.get("MINIO_ROOT_USER", "minioadmin")
RUSTFS_SECRET = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")
S3_REGION = os.environ.get("S3_REGION", "us-east-1")

GREEN, RED, YELLOW, DIM, RESET = "\033[32m", "\033[31m", "\033[33m", "\033[2m", "\033[0m"


@dataclass
class Demo:
    name: str
    location: str  # "local" or "s3"
    uri: str  # local subdir under data/, or "bucket/prefix" for s3
    expect_rows: int | None = None
    expect_distinct_key: tuple[str, int] | None = None  # (column, count)
    expect_deletion_vectors: bool = False
    needs: str = ""


DEMOS: dict[str, Demo] = {
    "01_path_append": Demo(
        name="01_path_append", location="local", uri="01_path_append",
        expect_rows=2000,
    ),
    "02_upsert_pk_mor": Demo(
        name="02_upsert_pk_mor", location="local", uri="02_upsert_pk_mor",
        expect_distinct_key=("user_id", 20), expect_deletion_vectors=True,
    ),
    "06_upsert_kafka_cdc": Demo(
        name="06_upsert_kafka_cdc", location="local", uri="06_upsert_kafka_cdc",
        expect_deletion_vectors=True, needs="kafka profile + just demo-upsert-kafka",
    ),
}

DEFAULT_SET = ["01_path_append", "02_upsert_pk_mor"]


def _filesystem(demo: Demo) -> tuple[pafs.FileSystem, str]:
    """Return (filesystem, base_path) for a demo's table root."""
    if demo.location == "local":
        return pafs.LocalFileSystem(), str(DATA_DIR / demo.uri)
    host = S3_ENDPOINT.split("://", 1)[-1]
    fs = pafs.S3FileSystem(
        access_key=RUSTFS_KEY,
        secret_key=RUSTFS_SECRET,
        endpoint_override=host,
        scheme="http" if S3_ENDPOINT.startswith("http://") else "https",
        region=S3_REGION,
    )
    return fs, demo.uri


def _read_commit_json(fs: pafs.FileSystem, base: str) -> list[dict]:
    """Return all parsed action objects across the ordered _delta_log commits."""
    log_dir = f"{base}/_delta_log"
    infos = fs.get_file_info(pafs.FileSelector(log_dir, recursive=False))
    commits = sorted(i.path for i in infos if i.path.endswith(".json"))
    actions: list[dict] = []
    for path in commits:
        with fs.open_input_stream(path) as f:
            for line in f.readall().decode().splitlines():
                line = line.strip()
                if line:
                    actions.append(json.loads(line))
    return actions


def _add_files_and_dvs(actions: list[dict]) -> tuple[list[str], int]:
    """All distinct add-file paths, and a count of deletion-vector actions.

    With merge-on-read, a file is `remove`d and re-`add`ed carrying a deletion
    vector, so "adds minus removes" would wrongly drop still-present files. Every
    add-file parquet stays on disk (DVs don't delete data files), so we read the
    full deduped set of add paths. That is correct for our checks: append demos
    have a single add (exact row count), and the upsert demo keeps every key
    across versions (distinct-key count is unaffected by ignoring DVs).
    """
    added: dict[str, None] = {}
    dv_count = 0
    for a in actions:
        for kind in ("add", "remove"):
            entry = a.get(kind)
            if isinstance(entry, dict):
                if kind == "add":
                    added[entry["path"]] = None
                if entry.get("deletionVector"):
                    dv_count += 1
    return list(added), dv_count


def _read_table(fs: pafs.FileSystem, base: str, files: list[str]) -> pa.Table:
    tables = []
    for rel in files:
        # add.path is relative to the table root and may be URL-encoded.
        tables.append(pq.read_table(f"{base}/{unquote(rel)}", filesystem=fs))
    if not tables:
        return pa.table({})
    return pa.concat_tables(tables, promote_options="default")


def verify_one(demo: Demo) -> bool:
    header = demo.name + (f"  ({demo.needs})" if demo.needs else "")
    print(f"\n{'='*66}\n{header}\n{'='*66}")
    try:
        fs, base = _filesystem(demo)
        actions = _read_commit_json(fs, base)
        add_paths, dv_count = _add_files_and_dvs(actions)
        table = _read_table(fs, base, add_paths)
    except Exception as e:  # noqa: BLE001
        print(f"{RED}FAIL{RESET}  could not read table: {e}")
        return False

    nrows = table.num_rows
    print(f"{DIM}add_files={len(add_paths)}  rows_in_files={nrows}  "
          f"columns={table.column_names}  dv_actions={dv_count}{RESET}")

    ok = True
    if demo.expect_rows is not None:
        good = nrows == demo.expect_rows
        ok &= good
        print(f"{GREEN if good else RED}{'PASS' if good else 'FAIL'}{RESET}  "
              f"rows == {demo.expect_rows} (got {nrows})")

    if demo.expect_distinct_key is not None:
        col, expected = demo.expect_distinct_key
        distinct = len(set(table.column(col).to_pylist())) if col in table.column_names else -1
        good = distinct == expected
        ok &= good
        print(f"{GREEN if good else RED}{'PASS' if good else 'FAIL'}{RESET}  "
              f"distinct {col} == {expected} (got {distinct})")

    if demo.expect_deletion_vectors:
        has_dv = dv_count > 0
        ok &= has_dv
        print(f"{GREEN if has_dv else RED}{'PASS' if has_dv else 'FAIL'}{RESET}  "
              f"merge-on-read deletion vectors present ({dv_count} DV action(s))")

    return ok


def main(argv: list[str]) -> int:
    if not argv:
        targets = DEFAULT_SET
        print(f"{DIM}Verifying default (local) demos: {', '.join(targets)}{RESET}")
    elif argv == ["all"]:
        targets = list(DEMOS)
    else:
        targets = argv

    results = {}
    for name in targets:
        name = name.removesuffix(".sql")
        demo = DEMOS.get(name)
        if demo is None:
            print(f"{YELLOW}skip{RESET} unknown demo '{name}' (known: {', '.join(DEMOS)})")
            results[name] = False
            continue
        results[name] = verify_one(demo)

    print(f"\n{'='*66}\nSummary\n{'='*66}")
    all_ok = True
    for name, ok in results.items():
        all_ok &= ok
        print(f"  {GREEN + 'PASS' + RESET if ok else RED + 'FAIL' + RESET}  {name}")
    return 0 if all_ok else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
