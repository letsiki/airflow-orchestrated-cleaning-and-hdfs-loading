# Data Engineering Assessment

## General Interpretation of the task at hand

- We are receiving incremental-transactional data twice a day
- Client data is not incremental, it may contain the whole subscribers dataset, but just to be sure we will be inserting only new sub_id's starting from phase 2
- First, we need to clean both sources.
- My understanding is that we need to maintain a db table of subscribers, overwriting it each time, with the new data.
- On the other hand, transactions, enriched by the joining with the
subscribers table, will be stored in a columnar format ready for analytics (parquet)

## Current Status: Phase 5: [Airflow Orchestration](docs/phase5-notes.md) ✅

## Phase History 
 
I would recommend that you stay in this branch (airflow) and follow the
instructions below to run the project. It is the easiest way (least prerequisites) to run the project.  

Having said that, I would encourage you to go through the notes of each phase, in order to get a more clear picture of how the project evolved, as each phase is building on top of the previous one. (Links below)

In case you still wish to run a previous phase of the project, please switch to branch main, and checkout to the appropriate tag (phase1, phase2, phase3, phase4)

- ✅ Phase 1: [Core Implementation (Base Requirements)](docs/phase1-notes.md)
- ✅ Phase 2: [Automation Features (Extra 1)](docs/phase2-notes.md)
- ✅ Phase 3: [Containerization & PostgreSQL (Extra 2)](docs/phase3-notes.md)
- ✅ Phase 4: [Hadoop Cluster & HDFS (Extra 3)](docs/phase4-notes.md)
- ✅ Phase 5: [Airflow Orchestration](docs/phase5-notes.md)

## Quick Start Guide
```bash
# Start infrastructure services (PostgreSQL + Hadoop cluster)
docker compose up -d

# Start Airflow services (connects to existing infrastructure)
cd airflow
docker compose up -d

# Access Airflow Web UI: http://localhost:8080 (might have to wait for 1-minute max)

# Enable optasia_pipeline (toggle)

# Click on it and then navigate to 'Runs'

# Choose active run and watch its progress

# Once the dag run is complete...

# You can find the parquet file(s) at NameNode Web UI: http://localhost:9870

# And also locally, at data/output

```

## Universal Tools
- PyLint - for linting python scripts
- Black - for formatting python scripts
- VsCode - IDE

## Other Projects
- [Youtube Keyword-Based Sentiment Trend Analysis](https://github.com/letsiki/youtube-keyword-based-sentiment-trend-analysis)
- [End-to-End Data Pipeline - American Community Survey](https://github.com/letsiki/end-to-end-data-pipeline-acs)