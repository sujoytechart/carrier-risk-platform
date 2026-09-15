# 0003. Six-month inspection and label windows

Date: 2026-09-02
Status: Accepted

## Context

The public FMCSA inspection file contains a rolling three years of data. A check
on 2026-09-02 found 8,278,182 rows spanning 2023-09-01 through 2026-08-30.

The original design used 24 months of inspection features and a 12-month label.
Together they consume the full 36 months before allowing any grace for late
reports. That leaves only one complete scoring date, which is not enough for a
time-based train and test split.

## Decision

- Inspection features, including violation and out-of-service totals already on
  each inspection row: previous **6 months**
- Crash features and the naive baseline: previous **24 months**
- Label: at least one deduplicated federal-recordable crash in the following
  **6 months**
- Scoring grid: monthly

With `D` months of inspection history, `F` months of inspection features, `L`
months of labels, and `G` months of reporting grace, the approximate number of
complete monthly scoring dates is `D - F - L - G`. At `D = 36`, `F = 6`, and
`L = 6`, that gives 21 dates when grace is three months and 15 dates when grace
is nine months. The longer crash lookback is possible because the Crash File
reaches back to 1990.

Proceed only if the measured grace is nine months or less. The label window is
not to be shortened below six months to make the arithmetic fit.

## Consequences

- At least 15 monthly scoring dates when grace is within the nine-month limit,
  compared with one under the original design. A shorter measured grace may
  provide more dates.
- The actual number of carrier-month rows is measured after applying eligibility.
  It is not estimated from the total carrier registry.
- The final three scoring dates form the held-out test. The preceding six dates
  are discarded so training and test label windows cannot overlap. The model
  specification is frozen before this test. There is no random split.
- A six-month inspection lookback may carry less signal per row than two years.
  The shorter window provides enough independent dates for a defensible test.

## Revisit if

Locally accumulated snapshots extend usable depth beyond the rolling three-year
public window, or a longer historical source is licensed.
