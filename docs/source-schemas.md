# Source schema notes

Verified against the live FMCSA endpoints on 2026-09-02. Government portals
change. Re-run these queries before trusting anything below.

| Feed | Socrata id | Rows | Columns |
|---|---|---:|---:|
| Crash File | `aayw-vxb3` | 4,985,509 | 59 |
| Vehicle Inspection File | `fx4q-ay7w` | 8,278,182 | 63 |

## Historical depth

```
$select=count(*), min(insp_date), max(insp_date)   on fx4q-ay7w
  -> 8278182, 20230901, 20260830
```

Confirms the rolling three-year inspection window. See
[ADR 0003](adr/0003-six-month-feature-and-label-windows.md).

## Carrier attribution on crashes

```
$select=count(*) as total, count(dot_number) as with_dot   on aayw-vxb3
  -> total 4,985,509 | with_dot 3,614,640
```

1,370,869 rows (27.497%) carry no USDOT number and cannot be attributed to a
carrier. See [ADR 0004](adr/0004-crash-eligibility-and-deduplication.md).

`no_id_flag` sums to 406,001, which is a different and much smaller population.
It is not a proxy for a missing USDOT and must not be used as the eligibility
filter.

## Types are not what the column names suggest

Almost every field arrives as `text`, including numerics and dates. Parsing is
the ingestion layer's job and the expectations belong in the feed schema files.

| Concern | Detail |
|---|---|
| Event dates | `insp_date` and `report_date` are `text` in `YYYYMMDD` form, for example `20230901` |
| Audit timestamps | `add_date`, `mcmis_add_date`, and `change_date` are `text` with values such as `20230929 2140`. Ingestion must parse these separately from event dates |
| `dot_number` | **`text` on crashes, `number` on inspections.** Conform to one representation before any join, and do not invent leading-zero semantics |
| Counts | `fatalities`, `injuries`, `vehicles_in_accident` are `text` |
| Flags | `tow_away`, `federal_recordable`, `state_recordable` are `text`, not boolean |

## Violation counts are already on the inspection row

`fx4q-ay7w` carries `viol_total` and `oos_total`, plus `driver_viol_total`,
`driver_oos_total`, `vehicle_viol_total`, `vehicle_oos_total`,
`hazmat_viol_total`, and `hazmat_oos_total`.

Every violation and out-of-service feature in the v0 model is derived from the
inspection feed alone. V0 therefore does not ingest a separate violation feed.
That feed would only be added for a named feature that needs a violation code,
regulation, severity, or other row-level detail.

## Knowledge-time candidates

| Feed | Fields |
|---|---|
| Crash | `add_date`, `upload_date`, `transaction_date`, `change_date` |
| Inspection | `mcmis_add_date`, `snet_input_date`, `upload_date`, `transaction_date`, `change_date` |

None of these is documented as "the date this became publicly visible". Which
field the platform treats as knowledge time, and the conservative shift applied
to it, is recorded alongside the ingestion schema rather than assumed here.

## Identity fields

| Feed | Stable key | Present |
|---|---|---|
| Crash | `crash_id` | yes, `number` |
| Crash fallback | `report_state`, `report_number`, `report_date`, `report_time`, `report_seq_no` | all present |
| Inspection | `inspection_id` | yes, `number` |
