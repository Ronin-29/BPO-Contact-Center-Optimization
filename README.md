# BPO Contact Center Optimization Pipeline

An end-to-end data engineering and analytics solution built on Microsoft Fabric. This project processes multi-year customer interaction data using a Medallion Architecture, optimizing query performance via Direct Lake mode, and enforcing robust data governance.

## 🏗️ Architecture Overview
The pipeline follows a structured Medallion Architecture managed natively via Microsoft Fabric Git Integration:
* **Bronze Layer:** Parameterized incremental ingestion of raw operational files into Delta tables using Data Factory.
* **Silver Layer:** Idempotent cleaning, data-quality auditing, and automated quarantine routing using PySpark.
* **Gold Layer:** Dimensional modeling transforming cleaned interaction logs into a high-performance Star Schema.
* **Semantic Layer:** Direct Lake semantic model with measure-only DAX expressions and Row-Level Security (RLS).

---

## 🛠️ Tech Stack & Core Tools
* **Orchestration & Ingestion:** Microsoft Fabric Data Factory (Pipelines & Copy Activities) ✅
* **Compute & Engineering:** Apache Spark (PySpark Notebooks) ✅
* **Storage Engine:** Delta Lake (Parquet format) ✅
* **Modeling & Security:** Tabular Editor 3 & TMDL (Tabular Model Definition Language) 🚧
* **Performance Tuning:** DAX Studio 🚧
* **DevOps:** Fabric Git Integration & Deployment Pipelines ✅

---

## 📈 Project Status & Metrics Log
*This section serves as a living log and will be populated with empirical results during pipeline execution.*

### Phase 0: Data Augmentation ✅
- **Goal:** Turn a 5,000-row messy seed CSV into a 180,000-row synthetic source, reproducibly.
- **Method:** Loaded the seed as strings, cast `Talk_Time_Sec` with `try_cast` (Spark 4.1 runs in ANSI mode, so a plain `cast` would fail on bad values), cross-joined with a 2024-01-01 to 2026-12-31 date spine, shuffled and limited to 180,000 rows, regenerated `Talk_Time_Sec` from the seed's mean and standard deviation, and injected anomalies (~2% `CSAT_Rating` set to "Five", ~1% `Agent_ID` set to null).
- **Reproducibility:** `SEED = 42`.
- **Output:** CSV with header in `Files/landing/bronze_customer_tickets_raw/`.
- **Verified:** 180,000 rows read back; 5,376 rows with `CSAT_Rating = 'Five'`; 3,704 null `Agent_ID`. Both counts are above the injected rates because the seed already had some dirty values.
- **Known limitation:** Both injections used the same random seed, so the two anomalies probably overlap heavily.

### Phase 1: Bronze Ingestion Pipeline ✅
- **Goal:** Load raw ticket data into Bronze incrementally, so each run only picks up rows newer than the last load.
- **Method:** Pipeline `pl_ingest_bpo_tickets` runs 4 activities in order: `cp_http_to_staging` (HTTP CSV to `dbo.stg_customer_tickets`, Overwrite), `lkp_get_watermark` (reads `control.load_log`), `cp_staging_to_bronze` (T-SQL filter `try_cast(sim_date AS DATE) > watermark`, Append to `dbo.bronze_customer_tickets`), and `nb_update_watermark` (runs `nb_setup_control_table` to save the new newest date).
- **Watermark design:** `control.load_log` holds exactly one row, upserted with `MERGE INTO` inside `update_watermark()`. The Lookup activity can only read a table's first row, so one row keeps the read simple.
- **Parameters:** `p_source_path`, `p_load_date`, `p_environment`.
- **Resilience:** Retry 2 times, 60 seconds apart, on the notebook activity, because trial Spark capacity sometimes throttles (HTTP 430).
- **Verified:** 180,000 rows in staging and Bronze; watermark = 2026-12-31 with `rows_loaded` = 180,000. A 2,000-row test batch dated January 2027 loaded only the new rows, and a rerun loaded 0.
- **Known limitation:** Bronze keeps the raw data as it arrived: 180,000 rows but only 5,000 distinct `Interaction_ID` values, because the repeats come from the Phase 0 source. Deduplication is handled in Silver. The failure alert path is not built yet.

### Why a Data Pipeline instead of Dataflow Gen2?
- A pipeline is built for orchestration: ordered steps, parameters, passing a value (the watermark) between activities, retries, and calling notebooks.
- Dataflow Gen2 is a low-code Power Query tool for shaping data. It cannot easily read a watermark, use it as a filter, and update it afterwards in one controlled flow.
- The pipeline moves and tracks the data. Transformations happen later in notebooks (Silver and Gold).

## Build Progress
- [X] Phase 0: Synthetic data augmentation
- [X] Phase 1: Bronze ingestion pipeline
- [ ] Phase 2: Silver cleaning
- [ ] Phase 2B: Gold star schema
- [ ] Phase 3: Direct Lake semantic model + RLS
- [ ] Phase 4: Performance profiling
- [ ] Phase 5: CI/CD & deployment      
---

## 📁 Repository Structure

Folder names such as `.Lakehouse`, `.Notebook`, and `.DataPipeline` are created automatically by Fabric Git integration. Only item definitions are stored here. Table data is not in Git.

```text
BPO-Contact-Center-Optimization/
├── BPO_Optimization_Lakehouse.Lakehouse/   # Lakehouse definition (Bronze/Silver/Gold tables live here)
├── BPO_Spark_Environment.Environment/      # Spark environment settings
├── nb_phase0_data_augmentation.Notebook/   # Phase 0: builds the 180,000-row synthetic source
├── pl_ingest_bpo_tickets.DataPipeline/     # Phase 1: parameterized incremental pipeline, HTTP source to Bronze Delta table
├── nb_setup_control_table.Notebook/        # Phase 1: creates control.load_log and update_watermark()
├── (planned) Silver and Gold notebooks     # Phases 2 and 2B
├── (planned) Semantic model                # Phase 3
└── README.md
```

**Source data:** the raw CSV that the pipeline downloads lives in a separate public repo, which acts as the external source system: [bpo-contact-center-fabric](https://github.com/Ronin-29/bpo-contact-center-fabric).
