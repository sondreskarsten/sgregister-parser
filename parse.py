"""Parser entrypoint: raw JSONL.gz → pool + firm_snapshots + area_snapshots + changelog.

Reads today's raw artefact ``raw/{date}/enterprises.jsonl.gz``
written by the collector, flattens each record into firm + area
rows, diffs against stored state, and emits the unified 12-col
changelog plus updated ``pool.parquet``, ``firm_snapshots.parquet``,
and ``area_snapshots.parquet``.

This job runs airgapped — no external APIs. All input comes from
``gs://{GCS_BUCKET}/{GCS_PREFIX}/raw/{date}/``.

Data flow
---------
1. Sync existing ``state/*.parquet`` from GCS to local disk.
2. Download today's raw JSONL.gz from GCS to local disk.
3. Flatten each record into firm row + area rows; dedup area rows.
4. Run :meth:`StateManager.diff_and_build`.
5. Write new pool, firm_snapshots, area_snapshots, ``changelog/{date}.parquet``.
6. Upload all back to GCS.

Environment variables
---------------------
GCS_BUCKET : str
    GCS bucket. Default ``sondre_brreg_data``. Empty for local-only.
GCS_PREFIX : str
    GCS path prefix. Default ``sgregister``.
RUN_MODE : str
    ``daily`` / ``bootstrap`` / ``backfill``. Default ``daily``.
RUN_DATE : str
    ISO date ``YYYY-MM-DD`` to parse. Default: today.
STATE_DIR : str
    Local working directory. Default ``/data``.
"""

import os
import sys
import json
import gzip
import uuid
from datetime import date

from flatten import flatten
from cdc import StateManager


GCS_BUCKET = os.environ.get("GCS_BUCKET", "sondre_brreg_data")
GCS_PREFIX = os.environ.get("GCS_PREFIX", "sgregister")
RUN_MODE = os.environ.get("RUN_MODE", "daily")
RUN_DATE = os.environ.get("RUN_DATE", "") or date.today().isoformat()
STATE_DIR = os.environ.get("STATE_DIR", "/data")


def gcs_bucket():
    if not GCS_BUCKET:
        return None
    from google.cloud import storage
    return storage.Client().bucket(GCS_BUCKET)


def download(gcs_path, local_path, bucket):
    if bucket is None:
        return os.path.exists(local_path)
    blob = bucket.blob(gcs_path)
    if not blob.exists():
        return False
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    blob.download_to_filename(local_path)
    print(f"  Downloaded gs://{GCS_BUCKET}/{gcs_path} ({os.path.getsize(local_path):,} bytes)", flush=True)
    return True


def upload(local_path, gcs_path, bucket):
    if bucket is None or not os.path.exists(local_path):
        return
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(local_path)
    print(f"  Uploaded gs://{GCS_BUCKET}/{gcs_path} ({os.path.getsize(local_path):,} bytes)", flush=True)


def sync_state_from_gcs(bucket):
    for name in ("pool.parquet", "firm_snapshots.parquet", "area_snapshots.parquet"):
        download(f"{GCS_PREFIX}/state/{name}", os.path.join(STATE_DIR, name), bucket)


def sync_state_to_gcs(bucket, run_date):
    for name in ("pool.parquet", "firm_snapshots.parquet", "area_snapshots.parquet"):
        upload(os.path.join(STATE_DIR, name), f"{GCS_PREFIX}/state/{name}", bucket)
    upload(
        os.path.join(STATE_DIR, "changelog", f"{run_date}.parquet"),
        f"{GCS_PREFIX}/cdc/changelog/{run_date}.parquet",
        bucket,
    )


def read_raw_jsonl(run_date, bucket):
    local_path = os.path.join(STATE_DIR, "raw", run_date, "enterprises.jsonl.gz")
    if bucket is not None:
        download(f"{GCS_PREFIX}/raw/{run_date}/enterprises.jsonl.gz", local_path, bucket)
    if not os.path.exists(local_path):
        raise FileNotFoundError(f"Raw file missing: {local_path}")
    records = []
    with gzip.open(local_path, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            records.append(json.loads(line))
    return records


def flatten_all(records):
    """Flatten raw envelopes into firm rows + area rows.

    Returns
    -------
    tuple
        ``(firm_rows, area_rows, dup_conflicts)`` — firm_rows is one
        row per record with a valid orgnr, area_rows is the flat
        union of all area rows, dup_conflicts counts records that
        had field-level conflicts between deduped area rows.
    """
    firm_rows = []
    area_rows = []
    dup_conflicts = 0
    for rec in records:
        firm, areas, conflicts = flatten(rec)
        if not firm.get("orgnr"):
            continue
        firm_rows.append(firm)
        area_rows.extend(areas)
        dup_conflicts += conflicts
    return firm_rows, area_rows, dup_conflicts


def run():
    run_id = str(uuid.uuid4())
    print(f"[{RUN_DATE}] sgregister-parser run_id={run_id} mode={RUN_MODE}", flush=True)

    os.makedirs(STATE_DIR, exist_ok=True)
    bucket = gcs_bucket()

    sync_state_from_gcs(bucket)

    records = read_raw_jsonl(RUN_DATE, bucket)
    print(f"  raw records: {len(records)}", flush=True)

    firm_rows, area_rows, dup_conflicts = flatten_all(records)
    print(f"  firm rows: {len(firm_rows)}  area rows (deduped): {len(area_rows)}  dup_conflicts: {dup_conflicts}", flush=True)

    state = StateManager(STATE_DIR)
    changelog_rows = state.diff_and_build(firm_rows, area_rows, RUN_DATE, run_id, RUN_MODE)

    by_kind = {}
    for row in changelog_rows:
        key = (row["event_subtype"], row["event_type"])
        by_kind[key] = by_kind.get(key, 0) + 1
    print(f"  events: {dict(sorted(by_kind.items()))}", flush=True)

    paths = state.save(changelog_rows, RUN_DATE)
    print(f"  wrote firm={paths['firm_rows']} area={paths['area_rows']} pool={paths['pool_rows']} changelog={paths['changelog_rows']}", flush=True)

    sync_state_to_gcs(bucket, RUN_DATE)
    print("done.", flush=True)


if __name__ == "__main__":
    run()
