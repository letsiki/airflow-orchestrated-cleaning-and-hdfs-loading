# Data Engineering Assessment

## General Interpretation of the Task at Hand
- We receive **transactional data** twice a day (incremental).  
- Subscriber data may be a **full snapshot** (spanning across years), but since this is uncertain, we **only insert new subscriber IDs** into the subscribers table.  
- Both data sources are cleaned before ingestion.  
- The subscribers table is **append-only**, growing as new IDs appear.  
- Transactions, enriched by joining with the current subscribers table, are stored in **Parquet format** (columnar, analytics-ready).  

---

## Current Status
**Phase 5 – [Airflow Orchestration](docs/phase5-notes.md) ✅**  
(DAG structure and flow diagram included in Phase 5 documentation)  

---

## Phase History
This repository shows the project’s evolution in 5 phases.  
- The **default branch is `airflow`** (Phase 5).  
- For earlier phases:  
  1. Switch to branch `main`  
  2. Checkout the appropriate tag (`phase1`, `phase2`, `phase3`, `phase4`)  

Each phase builds on the previous one, with detailed notes in the `/docs` folder:  
- ✅ Phase 1: [Core Implementation (Base Requirements)](docs/phase1-notes.md)  
- ✅ Phase 2: [Automation Features (Extra 1)](docs/phase2-notes.md)  
- ✅ Phase 3: [Containerization & PostgreSQL (Extra 2)](docs/phase3-notes.md)  
- ✅ Phase 4: [Hadoop Cluster & HDFS (Extra 3)](docs/phase4-notes.md)  
- ✅ Phase 5: [Airflow Orchestration](docs/phase5-notes.md)  

---

## Quick Start Guide
```bash
# Start infrastructure (PostgreSQL + Hadoop)
docker compose up -d

# Start Airflow (from airflow/ folder)
cd airflow && docker compose up -d

# Open Airflow UI (http://localhost:8080), enable DAG: optasia_pipeline

# Trigger a run and monitor under 'Runs'

# Outputs: 
# - HDFS NameNode UI: http://localhost:9870
# - Local folder: data/output/
```

---

## Assumptions
- Subscriber data may be a **full snapshot**, but we treat it cautiously:  
  → **only new subscriber IDs are inserted** into the subscribers table.  
- Transactions are **incremental** and arrive twice daily.  
- The subscribers table is **append-only** (no overwrites).  
- Cleaned and enriched transactions are stored in **Parquet format** for downstream analytics.  

---

## Universal Tools
- **PyLint** – linting Python scripts  
- **Black** – formatting Python scripts  
- **VS Code** – IDE  

---

## Other Projects
- [YouTube Keyword-Based Sentiment Trend Analysis](https://github.com/letsiki/youtube-keyword-based-sentiment-trend-analysis)  
- [End-to-End Data Pipeline – American Community Survey](https://github.com/letsiki/end-to-end-data-pipeline-acs)  
