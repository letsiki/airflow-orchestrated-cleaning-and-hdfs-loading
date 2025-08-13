# Data Engineering Assessment

## General Interpretation of the task at hand

- We are receiving incremental-transactional data twice a day
- Client data is not incremental, it can safely replace existing subscriber data.
- First, we need to clean both sources.
- My understanding is that we need to maintain a db table of subscribers, overwriting it each time, with the new data.
- On the other hand, transactions, enriched by the joining with the
subscribers table, will be stored in a columnar format ready for analytics (parquet)

## Current Status: Phase 2: [Automation Features (Extra 1)](docs/phase2-notes.md) ✅

## Phase History 
Click on the link for details and setup guide.  

Even if only testing the final phase, I would encourage you to go through the notes of each phase, in order to get a more clear picture of how the project evolved.  

- ✅ Phase 1: [Core Implementation (Base Requirements)](docs/phase1-notes.md)
- 🔄 Phase 2: [Automation Features (Extra 1)](docs/phase2-notes.md)
- 🔄 Phase 3: [Dockerization & PostgreSQL (Extra 2)](docs/phase3-notes.md)
- 🔄 Phase 4: [Hadoop Cluster & HDFS (Extra 3)](docs/phase4-notes.md)

## Universal Tools
- Pylint - for linting python scripts
- Black - for formatting python scripts
- VsCode - IDE
