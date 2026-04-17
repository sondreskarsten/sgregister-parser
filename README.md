# sgregister-parser

Airgapped parser for **SGregister** raw artefacts. Reads the
collector's daily `raw/{date}/enterprises.jsonl.gz`, diffs against
stored state, emits the platform's unified 12-column changelog plus
pool and snapshots.

Data source: see `sgregister-collector`.

## LUAS

`orgnr` in every row = the legal entity holding (or previously
holding) sentral godkjenning. Single-column LUAS. `document_id =
orgnr`. One row per legal entity per event.

## Pattern

Pattern B — self-contained changelog. `details_json` carries the
full current and previous flat records plus a structured `delta`
for modified events. Values are embedded; consumers never need to
join snapshots to recover them.

## Artefacts

```
gs://sondre_brreg_data/sgregister/
├── raw/{date}/                       (produced by sgregister-collector)
├── state/pool.parquet                6 cols — all orgnrs ever observed
├── state/snapshots.parquet           31 cols — latest state per orgnr
└── cdc/changelog/{date}.parquet      unified 12-col — one file per run_date
```

Pool and snapshots are **overwritten each run**, matching
`foretakshendelser`, `losore`, `doffin` convention. History lives in
`cdc/changelog/` (append-only across time).

## Event types

| event_type | when |
|---|---|
| `new` | orgnr first observation, not in pool |
| `reappeared` | orgnr in pool but absent from yesterday's snapshots |
| `modified` | content_hash differs between yesterday and today |
| `disappeared` | orgnr in yesterday's snapshots but missing from today's raw |

`event_subtype` is always `update`. Change specifics live in
`changed_fields` (JSON array of top-level field names) and
`details_json.delta`.

## Snapshot schema (31 cols)

`orgnr`, `name`, `phone`, `email`, `www`,
`biz_addr_{line_1..4, country, postal_code, postal_town}`,
`post_addr_{line_1..4, country, postal_code, postal_town}`,
`approved`, `approval_period_to`, `approval_certificate_url`,
`liability_insurance`, `industrial_injury_insurance`,
`educational_enterprise_approved`,
`n_approval_areas`, `approval_areas_json`, `approval_area_codes`,
`content_hash`, `first_seen`, `last_seen`.

`approval_area_codes` is a sorted list of canonical
`"{function_xml}-{subject_area_xml}-{grade}"` codes. The full
nested `valid_approval_areas[]` is preserved as JSON in
`approval_areas_json`.

## Pool schema

`orgnr`, `first_seen`, `last_seen`, `currently_in_universe`,
`n_appearances_total`, `n_gaps`. Never removed; `n_gaps` increments
on every `disappeared` event.

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
