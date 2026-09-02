# 0002. Historical carrier attributes excluded from features

Date: 2026-09-02
Status: Accepted

## Context

The original design used fleet size, driver count, operating status, authority
status, and safety rating as features. Using any of them for a past scoring date
requires the historical version that was available on that date.

FMCSA publishes current carrier data but no confirmed public archive of its
historical versions. The census and SMS inputs are overwritten snapshots, and
the old versions do not have a usable row-level knowledge timestamp. A FOIA
request, custom extract, or commercial archive may provide history later, but
the project cannot depend on data it does not have.

Using today's carrier attributes for an old scoring date would leak future
information into training. Preventing that leakage is the main purpose of this
project.

## Decision

Exclude carrier attributes from `v0` features entirely. Model events only:
inspections, violations, and crashes, each carrying its own occurrence date and
its own knowledge time.

Begin daily immutable snapshots of all three event feeds in Phase 0. This does
not recover missing history, but it starts building trustworthy observed history
from day one.

## Consequences

- Features are event-derived: inspection counts, violation counts,
  out-of-service counts, crash counts, and rates derived from them.
- Population is restricted to carriers with at least one inspection visible in
  the six months before the scoring date. Outside that population, the API
  returns `insufficient_history` rather than a fabricated score.
- The central point-in-time claim holds for every row in the training set, which
  would not be true under the wider design.
- Each event records its evidence quality. Historical rows use a conservative
  proxy derived from the source add date; newly acquired rows use the observed
  snapshot time.

## Revisit if

A usable archive of historical carrier-data versions becomes available, or a
later phase starts immutable carrier-data snapshots and accumulates enough
history for recent scoring dates.
