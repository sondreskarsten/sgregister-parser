"""Parquet state management for SGregister CDC (two-LUAS design).

Two snapshot tables, one pool, one unified 12-col changelog.

Layout
------
``state/pool.parquet``
    One row per orgnr ever seen. Accumulating. Tracks firm lifecycle
    (first_seen, last_seen, currently_in_universe, n_appearances,
    n_gaps).

``state/firm_snapshots.parquet``
    **LUAS = orgnr.** One row per currently-approved firm. Identity,
    contact, address, status, terms.

``state/area_snapshots.parquet``
    **LUAS = (orgnr, function_xml, subject_area_xml).** One row per
    approval scope currently held. ``grade`` and ``pbl`` are tracked
    attributes; a grade change is a ``modified`` event at area grain.

``cdc/changelog/{date}.parquet``
    Unified 12-col. ``event_subtype`` is ``firm`` or ``area`` —
    downstream consumers can slice by grain. ``document_id`` is
    ``orgnr`` for firm events, ``orgnr|function_xml|subject_area_xml``
    for area events. ``orgnr`` is always the firm's orgnr, matching
    the platform's one-hop slicing convention.

Event semantics
---------------
Firm events (event_subtype='firm'):
    ``new``        — orgnr not in pool
    ``reappeared`` — orgnr in pool but absent from yesterday's firm_snapshots
    ``modified``   — firm content_hash differs between runs
    ``disappeared``— orgnr missing from today's raw dump

Area events (event_subtype='area'):
    ``new``        — area LUAS not present for this orgnr yesterday
                     (fires both when firm is new and when an existing
                     firm gains a scope)
    ``modified``   — area content_hash differs (typically grade change)
    ``disappeared``— area LUAS present yesterday, absent today (scope
                     removed — can happen without the firm disappearing)

A firm disappearing also emits disappeared events for every area
the firm previously held, so area-level deterioration signals stay
complete regardless of firm-level events.
"""

import os
import json
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq


POOL_SCHEMA = pa.schema([
    ("orgnr", pa.string()),
    ("first_seen", pa.string()),
    ("last_seen", pa.string()),
    ("currently_in_universe", pa.bool_()),
    ("n_appearances_total", pa.int32()),
    ("n_gaps", pa.int32()),
])

FIRM_SNAPSHOTS_SCHEMA = pa.schema([
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
    ("content_hash", pa.string()),
    ("first_seen", pa.string()),
    ("last_seen", pa.string()),
])

AREA_SNAPSHOTS_SCHEMA = pa.schema([
    ("orgnr", pa.string()),
    ("function_xml", pa.string()),
    ("subject_area_xml", pa.string()),
    ("function", pa.string()),
    ("subject_area", pa.string()),
    ("pbl", pa.string()),
    ("pbl_xml", pa.string()),
    ("grade", pa.string()),
    ("content_hash", pa.string()),
    ("first_seen", pa.string()),
    ("last_seen", pa.string()),
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


_FIRM_NON_DIFF = {"content_hash", "first_seen", "last_seen"}
_AREA_NON_DIFF = {"content_hash", "first_seen", "last_seen", "orgnr", "function_xml", "subject_area_xml"}
_AREA_KEY_COLS = ("function_xml", "subject_area_xml")


def _utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def _firm_delta(old, new):
    """Structured delta + changed-field list for a firm row."""
    changed = []
    old_vals = {}
    new_vals = {}
    for k in new.keys():
        if k in _FIRM_NON_DIFF:
            continue
        if old.get(k) != new.get(k):
            changed.append(k)
            old_vals[k] = old.get(k)
            new_vals[k] = new.get(k)
    return {"old_values": old_vals, "new_values": new_vals}, sorted(changed)


def _area_delta(old, new):
    """Structured delta + changed-field list for an area row."""
    changed = []
    old_vals = {}
    new_vals = {}
    for k in new.keys():
        if k in _AREA_NON_DIFF:
            continue
        if old.get(k) != new.get(k):
            changed.append(k)
            old_vals[k] = old.get(k)
            new_vals[k] = new.get(k)
    return {"old_values": old_vals, "new_values": new_vals}, sorted(changed)


def _firm_summary(event_type, row, changed_fields=None):
    name = row.get("name") or row.get("orgnr")
    if event_type == "new":
        return f"new SG approval: {name}"
    if event_type == "reappeared":
        return f"SG reapproved: {name}"
    if event_type == "disappeared":
        return f"SG lapsed or revoked: {name}"
    if event_type == "modified":
        return f"firm modified: {', '.join(changed_fields or [])}"
    return event_type


def _area_summary(event_type, row, changed_fields=None, delta=None):
    label = f"{row.get('function') or row.get('function_xml')} × {row.get('subject_area') or row.get('subject_area_xml')} (grade {row.get('grade')})"
    if event_type == "new":
        return f"area added: {label}"
    if event_type == "disappeared":
        return f"area removed: {label}"
    if event_type == "modified":
        if delta and "grade" in (changed_fields or []):
            old_g = delta["old_values"].get("grade")
            new_g = delta["new_values"].get("grade")
            return f"grade change: {row.get('function') or row.get('function_xml')} × {row.get('subject_area') or row.get('subject_area_xml')} grade {old_g}→{new_g}"
        return f"area modified ({', '.join(changed_fields or [])}): {label}"
    return event_type


class StateManager:
    """Parquet state manager for SGregister CDC (two-LUAS)."""

    DATA_SOURCE = "sgregister"

    def __init__(self, state_dir):
        self.state_dir = state_dir
        os.makedirs(state_dir, exist_ok=True)
        os.makedirs(os.path.join(state_dir, "changelog"), exist_ok=True)

        self._pool_path = os.path.join(state_dir, "pool.parquet")
        self._firm_path = os.path.join(state_dir, "firm_snapshots.parquet")
        self._area_path = os.path.join(state_dir, "area_snapshots.parquet")

        self._pool = self._read_or_empty(self._pool_path, POOL_SCHEMA)
        self._firm = self._read_or_empty(self._firm_path, FIRM_SNAPSHOTS_SCHEMA)
        self._area = self._read_or_empty(self._area_path, AREA_SNAPSHOTS_SCHEMA)

    def _read_or_empty(self, path, schema):
        if os.path.exists(path):
            return pq.read_table(path)
        return pa.table({name: [] for name in schema.names}, schema=schema)

    def _firm_by_orgnr(self):
        return {r["orgnr"]: r for r in self._firm.to_pylist()}

    def _area_by_luas(self):
        return {(r["orgnr"], r["function_xml"], r["subject_area_xml"]): r
                for r in self._area.to_pylist()}

    def _pool_by_orgnr(self):
        return {r["orgnr"]: r for r in self._pool.to_pylist()}

    def diff_and_build(self, firm_rows, area_rows, run_date, run_id, run_mode):
        """Compute firm + area events, rebuild snapshots, update pool.

        Parameters
        ----------
        firm_rows : list of dict
            Flat firm rows produced by :func:`flatten.flatten_firm`.
            Each has a ``content_hash``.
        area_rows : list of dict
            Flat area rows produced by :func:`flatten.flatten_areas`.
            Each has a ``content_hash``.
        run_date : str
            ISO ``YYYY-MM-DD``.
        run_id : str
            Per-execution UUID.
        run_mode : str
            ``daily`` / ``bootstrap`` / ``backfill``.

        Returns
        -------
        list of dict
            Unified 12-col changelog rows (firm + area, interleaved
            in computation order).
        """
        detected = _utc_now_iso()
        valid_time = f"{run_date}T00:00:00+00:00"

        prev_firm = self._firm_by_orgnr()
        prev_area = self._area_by_luas()
        prev_pool = self._pool_by_orgnr()

        today_firm_orgnrs = {r["orgnr"] for r in firm_rows}
        today_area_luas = {(r["orgnr"], r["function_xml"], r["subject_area_xml"]) for r in area_rows}
        prev_firm_orgnrs = set(prev_firm.keys())

        changelog_rows = []

        new_firm_rows = []
        for rec in firm_rows:
            orgnr = rec["orgnr"]
            old = prev_firm.get(orgnr)
            pool_row = prev_pool.get(orgnr)

            if old is None and pool_row is None:
                event_type = "new"
                delta = {"new_values": {k: v for k, v in rec.items() if k not in _FIRM_NON_DIFF}}
                changed = sorted(delta["new_values"].keys())
                first_seen = detected
            elif old is None and pool_row is not None:
                event_type = "reappeared"
                delta = {
                    "new_values": {k: v for k, v in rec.items() if k not in _FIRM_NON_DIFF},
                    "previously_seen_until": pool_row.get("last_seen"),
                }
                changed = sorted(delta["new_values"].keys())
                first_seen = pool_row.get("first_seen") or detected
            elif old["content_hash"] == rec["content_hash"]:
                event_type = None
                first_seen = old.get("first_seen") or detected
            else:
                delta, changed = _firm_delta(old, rec)
                event_type = "modified"
                first_seen = old.get("first_seen") or detected

            new_firm_rows.append({**rec, "first_seen": first_seen, "last_seen": detected})

            if event_type is not None:
                changelog_rows.append({
                    "orgnr": orgnr,
                    "document_id": orgnr,
                    "data_source": self.DATA_SOURCE,
                    "event_type": event_type,
                    "event_subtype": "firm",
                    "summary": _firm_summary(event_type, rec, changed),
                    "changed_fields": json.dumps(changed, ensure_ascii=False),
                    "valid_time": valid_time,
                    "detected_time": detected,
                    "details_json": json.dumps(delta, ensure_ascii=False, sort_keys=True),
                    "source_run_mode": run_mode,
                    "run_id": run_id,
                })

        for orgnr in prev_firm_orgnrs - today_firm_orgnrs:
            old = prev_firm[orgnr]
            delta = {
                "old_values": {k: v for k, v in old.items() if k not in _FIRM_NON_DIFF},
                "last_seen": old.get("last_seen"),
            }
            changelog_rows.append({
                "orgnr": orgnr,
                "document_id": orgnr,
                "data_source": self.DATA_SOURCE,
                "event_type": "disappeared",
                "event_subtype": "firm",
                "summary": _firm_summary("disappeared", old),
                "changed_fields": json.dumps(["approved"], ensure_ascii=False),
                "valid_time": valid_time,
                "detected_time": detected,
                "details_json": json.dumps(delta, ensure_ascii=False, sort_keys=True),
                "source_run_mode": run_mode,
                "run_id": run_id,
            })

        new_area_rows = []
        for rec in area_rows:
            luas = (rec["orgnr"], rec["function_xml"], rec["subject_area_xml"])
            doc_id = "|".join(luas)
            old = prev_area.get(luas)

            if old is None:
                event_type = "new"
                delta = {"new_values": {k: v for k, v in rec.items() if k not in _AREA_NON_DIFF}}
                changed = sorted(delta["new_values"].keys())
                first_seen = detected
            elif old["content_hash"] == rec["content_hash"]:
                event_type = None
                first_seen = old.get("first_seen") or detected
            else:
                delta, changed = _area_delta(old, rec)
                event_type = "modified"
                first_seen = old.get("first_seen") or detected

            new_area_rows.append({**rec, "first_seen": first_seen, "last_seen": detected})

            if event_type is not None:
                changelog_rows.append({
                    "orgnr": rec["orgnr"],
                    "document_id": doc_id,
                    "data_source": self.DATA_SOURCE,
                    "event_type": event_type,
                    "event_subtype": "area",
                    "summary": _area_summary(event_type, rec, changed, delta if event_type == "modified" else None),
                    "changed_fields": json.dumps(changed, ensure_ascii=False),
                    "valid_time": valid_time,
                    "detected_time": detected,
                    "details_json": json.dumps(delta, ensure_ascii=False, sort_keys=True),
                    "source_run_mode": run_mode,
                    "run_id": run_id,
                })

        for luas in set(prev_area.keys()) - today_area_luas:
            old = prev_area[luas]
            delta = {
                "old_values": {k: v for k, v in old.items() if k not in _AREA_NON_DIFF},
                "last_seen": old.get("last_seen"),
            }
            doc_id = "|".join(luas)
            changelog_rows.append({
                "orgnr": old["orgnr"],
                "document_id": doc_id,
                "data_source": self.DATA_SOURCE,
                "event_type": "disappeared",
                "event_subtype": "area",
                "summary": _area_summary("disappeared", old),
                "changed_fields": json.dumps(["grade"], ensure_ascii=False),
                "valid_time": valid_time,
                "detected_time": detected,
                "details_json": json.dumps(delta, ensure_ascii=False, sort_keys=True),
                "source_run_mode": run_mode,
                "run_id": run_id,
            })

        self._firm = pa.Table.from_pylist(new_firm_rows, schema=FIRM_SNAPSHOTS_SCHEMA)
        self._area = pa.Table.from_pylist(new_area_rows, schema=AREA_SNAPSHOTS_SCHEMA)
        disappeared_firm_orgnrs = prev_firm_orgnrs - today_firm_orgnrs
        self._pool = self._rebuild_pool(prev_pool, today_firm_orgnrs, disappeared_firm_orgnrs, detected)

        return changelog_rows

    def _rebuild_pool(self, prev_pool, today_orgnrs, disappeared_orgnrs, detected):
        pool_rows = []
        seen = set()
        for orgnr, row in prev_pool.items():
            was_in_universe = bool(row.get("currently_in_universe"))
            in_today = orgnr in today_orgnrs
            just_disappeared = orgnr in disappeared_orgnrs
            n_gaps = int(row.get("n_gaps") or 0)
            n_app = int(row.get("n_appearances_total") or 0)
            if in_today:
                if not was_in_universe:
                    n_app += 1
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
                "n_appearances_total": n_app,
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
        """Persist firm + area snapshots, pool, and changelog."""
        pq.write_table(self._firm, self._firm_path, compression="zstd")
        pq.write_table(self._area, self._area_path, compression="zstd")
        pq.write_table(self._pool, self._pool_path, compression="zstd")
        cl_path = os.path.join(self.state_dir, "changelog", f"{run_date}.parquet")
        cl_table = pa.Table.from_pylist(changelog_rows, schema=CHANGELOG_SCHEMA)
        pq.write_table(cl_table, cl_path, compression="zstd")
        return {
            "firm_snapshots_path": self._firm_path,
            "area_snapshots_path": self._area_path,
            "pool_path": self._pool_path,
            "changelog_path": cl_path,
            "firm_rows": self._firm.num_rows,
            "area_rows": self._area.num_rows,
            "pool_rows": self._pool.num_rows,
            "changelog_rows": cl_table.num_rows,
        }
