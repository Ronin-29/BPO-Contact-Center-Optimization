# BPO Contact Center Optimization Pipeline

An enterprise-grade, end-to-end data engineering and analytics solution built on Microsoft Fabric. This project processes multi-year customer interaction data using a Medallion Architecture, optimizing query performance via Direct Lake mode, and enforcing robust data governance.

## 🏗️ Architecture Overview
The pipeline follows a structured Medallion Architecture managed natively via Microsoft Fabric Git Integration:
* **Bronze Layer:** Parameterized incremental ingestion of raw operational files into Delta tables using Data Factory.
* **Silver Layer:** Idempotent cleaning, data-quality auditing, and automated quarantine routing using PySpark.
* **Gold Layer:** Dimensional modeling transforming cleaned interaction logs into a high-performance Star Schema.
* **Semantic Layer:** Direct Lake semantic model with measure-only DAX expressions and Row-Level Security (RLS).

---

## 🛠️ Tech Stack & Core Tools
* **Orchestration & Ingestion:** Microsoft Fabric Data Factory (Pipelines & Copy Activities)
* **Compute & Engineering:** Apache Spark (PySpark Notebooks)
* **Storage Engine:** Delta Lake (Parquet format)
* **Modeling & Security:** Tabular Editor 3 & TMDL (Tabular Model Definition Language)
* **Performance Tuning:** DAX Studio
* **DevOps:** Fabric Git Integration & Deployment Pipelines

---

## 📈 Project Status & Metrics Log
*This section serves as a living log and will be populated with empirical results during pipeline execution.*

### Phase 0: Data Augmentation
* **Random Reproducibility Seed:** `SEED = 42`
* **Target Synthetic Row Count:** 180,000 rows (3-year spine)

### Phase 2: 

## Build Progress
- [X] Phase 0: Synthetic data augmentation
- [] Phase 1: Bronze ingestion pipeline
- [] Phase 2: Silver cleaning
- [] Phase 2B: Gold star schema
- [] Phase 3: Direct Lake semantic model + RLS
- [] Phase 4: Performance profiling
- [] Phase 5: CI/CD & deployment      
---

## 📁 Repository Structure

├── .data-factory/        # Data Factory pipeline JSON templates
├── .fabric-notebooks/   # PySpark notebooks (Augmentation, Silver, Gold)
├── .gold-layer/         # Delta table Star Schema definitions
├── .semantic-model/     # TMDL semantic model definitions
├── .diagnostics/        # DAX Studio benchmark trace outputs
└── README.md
