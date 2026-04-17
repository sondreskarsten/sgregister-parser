# sgregister-parser

Airgapped parser for **SGregister** raw artefacts. Reads the
collector's daily `raw/{date}/enterprises.jsonl.gz`, diffs against
stored state, emits the platform's unified 12-column changelog plus
firm-level and area-level snapshots.

Data source: see `sgregister-collector`.

## Two LUAS

SGregister has genuinely two units of analysis:

| LUAS | What changes at this grain |
|---|---|
| **`orgnr`** (firm) | identity, contact, address, approval period, insurance flags, apprenticeship status |
| **`(orgnr, function_xml, subject_area_xml)`** (area) | grade (the deterioration signal); `pbl` is an attribute tracked here |

The two run in the same parser but write separate snapshot tables.
One unified 12-col changelog carries both event streams, distinguished
by `event_subtype` ∈ `{firm, area}`.

### Why `pbl` is an attribute, not a key component

Empirically (April 2026 sample of 1,500 firms, 5,150 areas) every
area is under `PBL 2016`. Zero firms hold multiple PBL versions.
Putting `pbl` in the LUAS would add zero discriminative power today.
It stays on the row as an attribute so if a PBL 2026 (or similar
regulatory revision) ever appears, it surfaces as an ordinary
attribute change that we can notice and react to.

### Why `function` matters

`Utførende` (executing) / `Prosjekterende` (designing) /
`Ansvarlig søker` (applicant) / `Uavhengig kontrollerende`
(controller) are legally distinct certifications. An engineer
certified to design may not be certified to execute. Distribution
in the sample: Utførende 59%, Prosjekterende 28%, Ansvarlig søker
8%, Uavhengig kontrollerende 4%.

### Area row dedup

~1% of firms return exact-duplicate area rows in
`valid_approval_areas[]` (same function, same subject_area, same
grade) — a data-entry artefact on the DiBK side. The parser dedupes
on `(function_xml, subject_area_xml)` at ingest. A `dup_conflicts`
counter surfaces duplicates where fields disagreed (worth logging;
non-zero would be anomalous).

## Pattern

Pattern B — self-contained changelog. `details_json` carries
structured `old_values` / `new_values` per event. Values are
embedded; consumers never need to join snapshots to recover them.

## Artefacts

```
gs://sondre_brreg_data/sgregister/
├── raw/{date}/                            (produced by sgregister-collector)
├── state/pool.parquet                     6 cols — all orgnrs ever observed
├── state/firm_snapshots.parquet           29 cols — LUAS=orgnr, current firms
├── state/area_snapshots.parquet           11 cols — LUAS=(orgnr,function_xml,subject_area_xml)
└── cdc/changelog/{date}.parquet           unified 12-col — one file per run_date
```

Snapshots are **overwritten each run**, matching `foretakshendelser`,
`losore`, `doffin` convention. History lives in `cdc/changelog/`.

## Event semantics

| `event_subtype` | `event_type` | Meaning |
|---|---|---|
| `firm` | `new` | Orgnr first observation, not in pool |
| `firm` | `reappeared` | Orgnr in pool but absent from yesterday's firm_snapshots |
| `firm` | `modified` | Firm content_hash differs between runs |
| `firm` | `disappeared` | Orgnr missing from today's raw |
| `area` | `new` | Area LUAS absent for this orgnr yesterday |
| `area` | `modified` | Area content_hash differs (typically grade change) |
| `area` | `disappeared` | Area LUAS present yesterday, absent today |

A firm disappearing emits firm-level `disappeared` + an area-level
`disappeared` for every area it previously held. The two streams
are independently queryable.

## `document_id`

- Firm events: `document_id = orgnr`
- Area events: `document_id = f"{orgnr}|{function_xml}|{subject_area_xml}"`

`orgnr` column is always the firm's orgnr on both streams (one-hop
slicing convention).

## Environment

| Var | Default |
|---|---|
| `GCS_BUCKET` | `sondre_brreg_data` |
| `GCS_PREFIX` | `sgregister` |
| `RUN_MODE` | `daily` |
| `RUN_DATE` | today's ISO date |
| `STATE_DIR` | `/data` |

## Schedule

Proposed: daily 07:50 Europe/Oslo, 10 min after the collector.
