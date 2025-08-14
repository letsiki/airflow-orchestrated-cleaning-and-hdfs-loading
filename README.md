# Data Engineering Assessment

## General Interpretation of the task at hand

- We are receiving incremental-transactional data twice a day
- Client data is not incremental, it may contain the whole subscribers dataset, but just to be sure we will be inserting only new sub_id's starting from phase 2
- First, we need to clean both sources.
- My understanding is that we need to maintain a db table of subscribers, overwriting it each time, with the new data.
- On the other hand, transactions, enriched by the joining with the
subscribers table, will be stored in a columnar format ready for analytics (parquet)

## Current Status: Phase 2: [Automation Features (Extra 1)](docs/phase2-notes.md) ✅

## Phase History 
Click on the link for details and setup guide.  

Even if only testing the final phase, I would encourage you to go through the notes of each phase, in order to get a more clear picture of how the project evolved, as each phase is building on top of the previous one.

- ✅ Phase 1: [Core Implementation (Base Requirements)](docs/phase1-notes.md)
- ✅ Phase 2: [Automation Features (Extra 1)](docs/phase2-notes.md)
- ✅ Phase 3: [Containerization & PostgreSQL (Extra 2)](docs/phase3-notes.md)
- 🔄 Phase 4: [Hadoop Cluster & HDFS (Extra 3)](docs/phase4-notes.md)
- 🔄 Phase 4: [Airflow Orchestration (Optional))](docs/phase4-notes.md)


## Universal Tools
- PyLint - for linting python scripts
- Black - for formatting python scripts
- VsCode - IDE
