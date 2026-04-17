"""Parquet state management for SGregister CDC.

Maintains three output families on disk (synced to/from GCS by the
entrypoint):

* ``state/pool.parquet`` — one row per orgnr ever observed with
  central approval. Accumulating: rows are never removed. Tracks
  first/last-seen timestamps and counts of appearances and gaps.
* ``state/snapshots.parquet`` — one row per orgnr currently in the
  approval universe. Overwritten each run. Carries the flattened
  30-field representation plus ``content_hash``, ``first_seen``,
  ``last_seen``.
* ``cdc/changelog/{date}.parquet`` — one file per run_date, unified
  12-column schema matching the platform contract.

Change detection
----------------
For every orgnr present in today's raw dump:

* **new** — orgnr not present in ``pool.parquet``.
* **reappeared** — orgnr in pool but not in yesterday's snapshots
  (it disappeared at some point and is now back).
* **modified** — orgnr in yesterday's snapshots and today's, with
  differing ``content_hash``.
* no event — orgnr in both with matching hash.

For orgnrs in yesterday's snapshots but not today's raw dump:

* **disappeared** — orgnr lost approval. Emitted once at the
  disappearance boundary; pool row is marked with an incremented
  ``n_gaps``.

``details_json`` carries the full current and previous flat records
plus a structured ``delta`` for ``modified`` events.
"""

import os
import json
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq


SNAPSHOTS_SCHEMA = pa.schema([
    ("orgnr", pa.string()),
    ("name", pa.string()),
    ("phone", pa.string()),
    ("email", pa.string()),
    ("www", pa.string()),
    ("biz_addr_line_1", pa.string()),
    ("biz_addr_line_2", pa.string()),
    ("biz_addr_line_3", pa.string()),
    ("biz_addr_line_4", pa.string()),
    ("biz_addr_country", pa.string()),
    ("biz_addr_postal_code", pa.string()),
    ("biz_addr_postal_town", pa.string()),
    ("post_addr_line_1", pa.string()),
    ("post_addr_line_2", pa.string()),
    ("post_addr_line_3", pa.string()),
    ("post_addr_line_4", pa.string()),
    ("post_addr_country", pa.string()),
    ("post_addr_postal_code", pa.string()),
    ("post_addr_postal_town", pa.string()),
    ("approved", pa.bool_()),
    ("approval_period_to", pa.string()),
    ("approval_certificate_url", pa.string()),
    ("liability_insurance", pa.bool_()),
    ("industrial_injury_insurance", pa.bool_()),
    ("educational_enterprise_approved", pa.bool_()),
    ("n_approval_areas", pa.int32()),
    ("approval_areas_json", pa.large_string()),
    ("approval_area_codes", pa.list_(pa.string())),
    ("content_hash", pa.string()),
    ("first_seen", pa.string()),
    ("last_seen", pa.string()),
])

POOL_SCHEMA = pa.schema([
    ("orgnr", pa.string()),
    ("first_seen", pa.string()),
    ("last_seen", pa.string()),
    ("currently_in_universe", pa.bool_()),
    ("n_appearances_total", pa.int32()),
    ("n_gaps", pa.int32()),
])

CHANGELOG_SCHEMA = pa.schema([
    ("orgnr", pa.string()),
    ("document_id", pa.string()),
    ("data_source", pa.string()),
    ("event_type", pa.string()),
    ("event_subtype", pa.string()),
    ("summary", pa.string()),
    ("changed_fields", pa.string()),
    ("valid_time", pa.string()),
    ("detected_time", pa.string()),
    ("details_json", pa.large_string()),
    ("source_run_mode", pa.string()),
    ("run_id", pa.string()),
])


_NON_DIFF_FIELDS = {"content_hash", "first_seen", "last_seen"}


def _utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def _compute_delta(old_flat, new_flat):
    """Return structured delta + list of changed field names.

    Parameters
    ----------
    old_flat : dict
        Previous snapshot row (with or without ``content_hash`` etc).
    new_flat : dict
        New snapshot row.

    Returns
    -------
    tuple
        ``(delta, changed_fields)`` where ``delta`` is a dict with
        ``old_values``, ``new_values``, ``areas_added``,
        ``areas_removed`` and ``changed_fields`` is a sorted list of
        top-level field names that differ.
    """
    changed = []
    old_vals = {}
    new_vals = {}
    for k in new_flat.keys():
        if k in _NON_DIFF_FIELDS:
            continue
        if old_flat.get(k) != new_flat.get(k):
            changed.append(k)
            old_vals[k] = old_flat.get(k)
            new_vals[k] = new_flat.get(k)

    old_codes = set(old_flat.get("approval_area_codes") or [])
    new_codes = set(new_flat.get("approval_area_codes") or [])
    delta = {
        "old_values": old_vals,
        "new_values": new_vals,
        "areas_added": sorted(new_codes - old_codes),
        "areas_removed": sorted(old_codes - new_codes),
    }
    return delta, sorted(changed)


def _summary_for_modified(changed_fields, delta):
    """Short human-readable summary for a modified event."""
    parts = []
    added = delta.get("areas_added") or []
    removed = delta.get("areas_removed") or []
    if added:
        parts.append(f"+{len(added)} areas")
    if removed:
        parts.append(f"-{len(removed)} areas")
    if "approval_period_to" in changed_fields:
        old = delta["old_values"].get("approval_period_to")
        new = delta["new_values"].get("approval_period_to")
        parts.append(f"period {old}→{new}")
    for flag in ("liability_insurance", "industrial_injury_insurance", "educational_enterprise_approved"):
        if flag in changed_fields:
            parts.append(flag)
    if not parts:
        parts.append(f"{len(changed_fields)} fields changed")
    return "modified: " + ", ".join(parts)


class StateManager:
    """Parquet state manager for SGregister CDC.

    Parameters
    ----------
    state_dir : str
        Local working directory. Expected layout::

            state_dir/
                pool.parquet
                snapshots.parquet
                changelog/
                    YYYY-MM-DD.parquet
    """

    DATA_SOURCE = "sgregister"

    def __init__(self, state_dir):
        self.state_dir = state_dir
        os.makedirs(state_dir, exist_ok=True)
        os.makedirs(os.path.join(state_dir, "changelog"), exist_ok=True)

        self._pool_path = os.path.join(state_dir, "pool.parquet")
        self._snap_path = os.path.join(state_dir, "snapshots.parquet")

        self._pool = self._read_or_empty(self._pool_path, POOL_SCHEMA)
        self._snap = self._read_or_empty(self._snap_path, SNAPSHOTS_SCHEMA)

    def _read_or_empty(self, path, schema):
        if os.path.exists(path):
            return pq.read_table(path)
        return pa.table({name: [] for name in schema.names}, schema=schema)

    def _snap_by_orgnr(self):
        """Return dict orgnr → flat dict for the current snapshot table."""
        df = self._snap.to_pylist()
        return {row["orgnr"]: row for row in df}

    def _pool_by_orgnr(self):
        df = self._pool.to_pylist()
        return {row["orgnr"]: row for row in df}

    def diff_and_build(self, flat_records, run_date, run_id, run_mode):
        """Compute events, rebuild snapshots, update pool.

        Parameters
        ----------
        flat_records : list of dict
            Flattened records from today's raw dump — one per
            currently-approved orgnr. Each must include
            ``content_hash``.
        run_date : str
            ISO date string ``YYYY-MM-DD`` representing today's
            observation date.
        run_id : str
            Per-execution UUID.
        run_mode : str
            ``daily`` / ``bootstrap`` / ``backfill``.

        Returns
        -------
        list of dict
            Unified 12-col changelog rows (may be empty).
        """
        detected = _utc_now_iso()
        valid_time = f"{run_date}T00:00:00+00:00"

        prev_snap = self._snap_by_orgnr()
        prev_pool = self._pool_by_orgnr()

        today_orgnrs = {r["orgnr"] for r in flat_records}
        prev_orgnrs = set(prev_snap.keys())

        changelog_rows = []
        new_snap_rows = []

        for rec in flat_records:
            orgnr = rec["orgnr"]
            old = prev_snap.get(orgnr)
            pool_row = prev_pool.get(orgnr)

            if old is None and pool_row is None:
                event_type = "new"
                summary = f"new SG approval: {rec.get('name')}"
                delta = {"new_values": {k: v for k, v in rec.items() if k not in _NON_DIFF_FIELDS}}
                changed = sorted(delta["new_values"].keys())
                first_seen = detected
            elif old is None and pool_row is not None:
                event_type = "reappeared"
                summary = f"SG reapproved: {rec.get('name')}"
                delta = {"new_values": {k: v for k, v in rec.items() if k not in _NON_DIFF_FIELDS},
                         "previously_seen_until": pool_row.get("last_seen")}
                changed = sorted(delta["new_values"].keys())
                first_seen = pool_row.get("first_seen") or detected
            elif old["content_hash"] == rec["content_hash"]:
                event_type = None
                first_seen = old.get("first_seen") or detected
            else:
                delta, changed = _compute_delta(old, rec)
                event_type = "modified"
                summary = _summary_for_modified(changed, delta)
                first_seen = old.get("first_seen") or detected

            new_snap_rows.append({
                **rec,
                "first_seen": first_seen,
                "last_seen": detected,
            })

            if event_type is not None:
                changelog_rows.append({
                    "orgnr": orgnr,
                    "document_id": orgnr,
                    "data_source": self.DATA_SOURCE,
                    "event_type": event_type,
                    "event_subtype": "update",
                    "summary": summary,
                    "changed_fields": json.dumps(changed, ensure_ascii=False),
                    "valid_time": valid_time,
                    "detected_time": detected,
                    "details_json": json.dumps(delta, ensure_ascii=False, sort_keys=True),
                    "source_run_mode": run_mode,
                    "run_id": run_id,
                })

        disappeared_orgnrs = prev_orgnrs - today_orgnrs
        for orgnr in disappeared_orgnrs:
            old = prev_snap[orgnr]
            delta = {"old_values": {k: v for k, v in old.items() if k not in _NON_DIFF_FIELDS},
                     "last_seen": old.get("last_seen")}
            changelog_rows.append({
                "orgnr": orgnr,
                "document_id": orgnr,
                "data_source": self.DATA_SOURCE,
                "event_type": "disappeared",
                "event_subtype": "update",
                "summary": f"SG lapsed or revoked: {old.get('name')}",
                "changed_fields": json.dumps(["approved"], ensure_ascii=False),
                "valid_time": valid_time,
                "detected_time": detected,
                "details_json": json.dumps(delta, ensure_ascii=False, sort_keys=True),
                "source_run_mode": run_mode,
                "run_id": run_id,
            })

        self._snap = pa.Table.from_pylist(new_snap_rows, schema=SNAPSHOTS_SCHEMA)
        self._pool = self._rebuild_pool(prev_pool, today_orgnrs, disappeared_orgnrs, detected)

        return changelog_rows

    def _rebuild_pool(self, prev_pool, today_orgnrs, disappeared_orgnrs, detected):
        """Recompute the pool table from prior pool + today's membership."""
        pool_rows = []
        seen = set()

        for orgnr, row in prev_pool.items():
            was_in_universe = bool(row.get("currently_in_universe"))
            in_today = orgnr in today_orgnrs
            just_disappeared = orgnr in disappeared_orgnrs
            n_gaps = int(row.get("n_gaps") or 0)
            n_appearances = int(row.get("n_appearances_total") or 0)
            if in_today:
                if not was_in_universe:
                    n_appearances += 1
                last_seen = detected
            else:
                if just_disappeared:
                    n_gaps += 1
                last_seen = row.get("last_seen") or detected
            pool_rows.append({
                "orgnr": orgnr,
                "first_seen": row.get("first_seen") or detected,
                "last_seen": last_seen,
                "currently_in_universe": in_today,
                "n_appearances_total": n_appearances,
                "n_gaps": n_gaps,
            })
            seen.add(orgnr)

        for orgnr in sorted(today_orgnrs - seen):
            pool_rows.append({
                "orgnr": orgnr,
                "first_seen": detected,
                "last_seen": detected,
                "currently_in_universe": True,
                "n_appearances_total": 1,
                "n_gaps": 0,
            })

        return pa.Table.from_pylist(pool_rows, schema=POOL_SCHEMA)

    def save(self, changelog_rows, run_date):
        """Persist snapshots, pool, and today's changelog to disk.

        Parameters
        ----------
        changelog_rows : list of dict
            Rows produced by :meth:`diff_and_build`.
        run_date : str
            ISO date used to name the changelog file.
        """
        pq.write_table(self._snap, self._snap_path, compression="zstd")
        pq.write_table(self._pool, self._pool_path, compression="zstd")
        cl_path = os.path.join(self.state_dir, "changelog", f"{run_date}.parquet")
        cl_table = pa.Table.from_pylist(changelog_rows, schema=CHANGELOG_SCHEMA)
        pq.write_table(cl_table, cl_path, compression="zstd")
        return {
            "snapshots_path": self._snap_path,
            "pool_path": self._pool_path,
            "changelog_path": cl_path,
            "snapshots_rows": self._snap.num_rows,
            "pool_rows": self._pool.num_rows,
            "changelog_rows": cl_table.num_rows,
        }
