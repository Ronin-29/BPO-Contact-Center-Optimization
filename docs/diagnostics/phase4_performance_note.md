# Phase 4: Performance profiling note

Tool: DAX Studio 3.6.1, connected through the XMLA endpoint to sm_bpo_contact_center
(Direct Lake on SQL endpoint, Direct Lake Only), signed in as the workspace admin.
Date: 4 Oct 2026. Fabric trial capacity. Data: 4,925 fact rows.
Method: Server Timings on. Clear on Run on for the five-run tests.
All numbers below are as read from DAX Studio. Units are milliseconds.

## Cold versus warm (query q2, DISTINCTCOUNT of Interaction_ID)

| Run | Total | FE | SE |
|---|---|---|---|
| First use of the column | 2,420 | 2,413 | 7 |
| Immediate rerun | 31 | 14 | 17 |

Result was 4,925 both times.

## Five warm runs (Clear on Run on)

| Query | Runs (Total) | Median | Range |
|---|---|---|---|
| q2 DISTINCTCOUNT Interaction_ID | 16, 32, 16, 17, 33 | 17 | 16 to 33 |
| q1 queue summary | 17, 17, 17, 31, 33 | 17 | 17 to 33 |

q1 result matched the report: Billing 1,655, General 816, Retention 811,
Tech Support 815, Unknown 828.

## What was tried and did not work
- Clear Cache did not produce a cold run (33 ms, SE 0).
- A model refresh in the workspace did not produce a cold run (33 ms).

## Limitations
- Only one cold sample. It cannot be repeated for the same column.
- The slow first run is shown as Formula Engine time. The reason is not confirmed.
  The likely cause is the column being loaded into memory on first use.
- Warm totals move in steps of about 16 ms. This is probably timer resolution
  (not confirmed), so differences of 16 ms are treated as noise.
- Small data (4,925 rows) on a trial capacity. These numbers do not predict
  performance at larger scale.
- Queries ran as the admin, so row-level security was not applied.

## Files
- q1_warm_queue_summary.dax
- q2_cold_column_distinctcount_interaction_id.dax
