# JDE Assessment

## General Interpretation of the task at hand

- We are receiving incremental-transactional data twice a day
- Client data is not incremental, it may contain the whole subscribers dataset, but just to be sure we will be inserting only new sub_id's starting from phase 2
- First, we need to clean both sources.
- My understanding is that we need to maintain a db table of subscribers, appending to it new-users only.
- On the other hand, transactions, enriched by the joining with the
subscribers table, will be stored in a columnar format (parquet) ready for analytics.

## Current Status: Phase 5: [Airflow Orchestration](docs/phase5-notes.md)

## Phase History 

Even if only testing the final phase, I would encourage you to go through the notes of each phase, in order to get a more clear picture of how the project evolved, as each phase is building on top of the previous one.

Phases 1-4 cover all the requirements. They are all located in the branch 'main'.  
If you wish you may visit branch 'airflow' and explore Phase 5.  
It is built upon Phase 4 and adds Orchestration and Scheduling to the Pipeline.

**Phases and documentation links:**

- ✅ Phase 1: [Core Implementation (Base Requirements)](docs/phase1-notes.md)
- ✅ Phase 2: [Automation Features (Extra 1)](docs/phase2-notes.md)
- ✅ Phase 3: [Containerization & PostgreSQL (Extra 2)](docs/phase3-notes.md)
- ✅ Phase 4: [Hadoop Cluster & HDFS (Extra 3)](docs/phase4-notes.md)

*Extra*:  
- 🔄 Phase 5: [Airflow Orchestration](docs/phase5-notes.md) (Available at branch 'airflow')


## Universal Tools
- PyLint - for linting python scripts
- Black - for formatting python scripts
- VsCode - IDE
