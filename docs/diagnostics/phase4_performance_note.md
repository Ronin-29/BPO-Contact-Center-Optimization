# Phase 4: performance note

Measured with DAX Studio's Benchmark tool on `sm_bpo_contact_center` (169,016 fact rows, Direct Lake on SQL endpoint, Direct Lake Only, trial capacity). 10 cold runs (cache cleared before each run) and 10 warm runs per query. Times are in milliseconds. The timer moves in steps of about 16 ms.

| Query | Cold average | Cold range | Warm average | Warm range | Storage engine average, cold | Storage engine, warm |
|---|---|---|---|---|---|---|
| Queue summary | 11.5 | 0 to 17 | 13.2 | 0 to 17 | 1.7 | 0 |
| Distinct count of Interaction_ID | 22.8 | 15 to 33 | 11.7 | 0 to 17 | 11.7 | 0 |
| Subtitle measure Sub Daily Range | 13.0 | 0 to 31 | 10.1 | 0 to 17 | 3.3 | 0 |

Reading: differences smaller than one timer step (about 16 ms) are noise. Only the distinct count shows a clear cold versus warm gap. "Cold" means the engine cache was cleared; the columns were already in memory, so this is not a measurement of loading a column from OneLake. Queries ran as admin, so row-level security was not applied.
