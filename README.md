# BPO Contact Center Optimization Pipeline

An end-to-end data engineering and analytics solution built on Microsoft Fabric. This project processes multi-year customer interaction data using a Medallion Architecture, optimizing query performance via Direct Lake mode, and enforcing robust data governance.

## 🏗️ Architecture Overview
The pipeline follows a structured Medallion Architecture managed natively via Microsoft Fabric Git Integration:
* **Bronze Layer:** Parameterized incremental in progress of raw operational files into Delta tables using Data Factory.
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

### Phase 1: 

## Build Progress
- [X] Phase 0: Synthetic data augmentation
- [ ] Phase 1: Bronze ingestion pipeline
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
├── pl_ingest_bpo_tickets.DataPipeline/     # Phase 1: parameterized pipeline, HTTP source to Bronze Delta table
├── nb_setup_control_table.Notebook/        # (planned) Phase 1: creates control.load_log
├── (planned) Silver and Gold notebooks     # Phases 2 and 2B
├── (planned) Semantic model                # Phase 3
└── README.md
```

**Source data:** the raw CSV that the pipeline downloads lives in a separate public repo, which acts as the external source system: [bpo-contact-center-fabric](https://github.com/Ronin-29/bpo-contact-center-fabric).
