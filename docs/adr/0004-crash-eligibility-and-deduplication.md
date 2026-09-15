# 0004. Crash eligibility rules and the carrier incident key

Date: 2026-09-02
Status: Accepted

## Context

Two properties of the crash file affect every label the platform produces.
Measured against the live table on 2026-09-02:

```
total rows        4,985,509
rows with USDOT   3,614,640
rows without      1,370,869   (27.50%)
```

The missing values are null, not empty strings. A crash without a USDOT number
cannot be assigned to a carrier.

The file can also contain one row for each commercial vehicle involved in a
crash. Counting rows would count one multi-vehicle incident more than once for
the same carrier.

## Decision

**Eligibility.** A crash contributes to carrier features and labels only when:

- its USDOT number is present and parses to a positive identifier.
- it is federally recordable.
- every field required by the incident key is valid.
- it was visible at the scoring date.
- its event version was not deleted at that time.

Ineligible rows are retained in the raw and clean layers carrying
`model_eligibility = 'excluded'` and an `exclusion_reason`. They are never
silently dropped during ingestion, and the exclusion rate is monitored per
batch.

**Two keys serve different purposes.** A physical source row uses `crash_id`.
If `crash_id` is absent, its fallback key is the tuple `(report_state,
report_number, report_date, report_time, report_seq_no)`.

The carrier-level incident key is a tuple that deliberately omits
`report_seq_no`:

```
carrier_crash_key = (
    dot_number,
    report_state,
    report_number,
    report_date,
    report_time
)
```

The stored key is generated from this tuple with an unambiguous encoding. Raw
string concatenation is not used. Aggregations take `max(fatalities)`,
`max(injuries)`, and `bool_or(tow_away)`. Labels use
`count(distinct carrier_crash_key)`.

## Consequences

- More than a quarter of crash rows do not have a carrier identifier and cannot
  enter carrier modeling. Exclusions remain visible and are counted by reason.
- Multi-vehicle incidents count once per carrier. The physical rows remain
  underneath, so the deduplication is auditable.
- The label means a carrier was involved in a federally recordable crash. FMCSA
  crash data records involvement, not fault, and the model is described
  accordingly.

## Revisit if

FMCSA changes crash file identity fields, or a supplementary source allows
attribution of currently unattributable crashes.
