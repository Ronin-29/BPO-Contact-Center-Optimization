# Phase 4: Performance profiling note

Tool: DAX Studio 3.6.1, connected through the XMLA endpoint to sm_bpo_contact_center
(Direct Lake on SQL endpoint, Direct Lake Only), signed in as the workspace admin.
Date: 4 Oct 2026. Fabric trial capacity. Data: 4,925 fact rows.
Method: Server Timings on. Clear on Run was on for the q2 runs and the five-run
tests (from the author's memory, not recorded run by run).
All numbers below are as read from DAX Studio. Units are milliseconds.
FE = Formula Engine time. SE = Storage Engine time.

## First use versus rerun (query q2, DISTINCTCOUNT of Interaction_ID)

| Run | Total | FE | SE |
|---|---|---|---|
| First use of the column | 2,420 | 2,413 | 7 |
| Immediate rerun | 31 | 14 | 17 |

Result was 4,925 both times. The first run was the first time any query used
this column, so it may be a cold run, but this is not proven (see Limitations).
The saved timings of the rerun and the warm screenshot both show the storage
engine answering from cache, even though Clear on Run was reported as on. The
reason is not confirmed. So part of the speed-up may come from query caching
and not only from the column already being in memory.

## Five warm runs (Clear on Run on)

| Query | Runs (Total) | Median | Range |
|---|---|---|---|
| q2 DISTINCTCOUNT Interaction_ID | 16, 32, 16, 17, 33 | 17 | 16 to 33 |
| q1 queue summary | 17, 17, 17, 31, 33 | 17 | 17 to 33 |

The warm screenshot of q2 (phase4_01_warm_run.png) shows 17 ms total, FE 17,
SE 0, SE Cache 100%.

q1 result matched the report: Billing 1,655, General 816, Retention 811,
Tech Support 815, Unknown 828.

## What was tried and did not work
- Clear Cache (tested on q1) did not produce a cold run (33 ms, SE 0).
- A model refresh in the workspace (tested on q1) did not produce a cold run
  (33 ms).

## Limitations
- Only one first-use sample. It could not be repeated for the same column with
  the two methods tried (Clear Cache and a model refresh). A column not used
  before might give a second sample, but this was not tried.
- The slow first run is shown as Formula Engine time. The reason is not
  confirmed. The likely cause is the column being loaded into memory on first
  use.
- Cache hits (SE Cache 100%) appeared on warm runs even with Clear on Run on.
  The reason is not confirmed, so the warm numbers may include query caching.
- Warm totals move in steps of about 16 ms. This is probably timer resolution
  (not confirmed), so differences of 16 ms are treated as noise.
- Small data (4,925 rows) on a trial capacity. These numbers do not predict
  performance at larger scale.
- Queries ran as the admin, so row-level security was not applied.
- The settings of each run (such as Clear on Run) were not written down at the
  time, so some are from memory.

## Files and evidence
In docs/diagnostics/:
- q1_warm_queue_summary.dax: the queue summary query.
- q2_cold_column_distinctcount_interaction_id.dax: the distinct count query.
- q1_warm_queue_summary.dax.serverTimings: saved timings of one warm q1 run
  (about 33 ms).
- q2_cold_column_distinctcount_interaction_id.dax.serverTimings: saved timings
  of the q2 rerun (31 ms). It does not contain the first slow run.
In docs/screenshots/:
- phase4_01_cold_run.png: the first slow run (2,420 ms). This is the only
  record of the cold run.
- phase4_01_warm_run.png: a warm run of q2 (17 ms total, FE 17, SE 0,
  SE Cache 100%).
