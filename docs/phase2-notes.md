## Phase 2: Automation Features (Extra 1)
- Implement retry logic for unmatched subscribers
- Add only new subscribers (no duplicates/overwrites)
- Move SQL queries to external files
- Create reusable utility functions
- Maintain local venv + SQLite setup

## New Features
- Store unmatched transactions for retry in subsequent runs
- Prevent duplicate subscribers
- Modular code structure with external SQL files and helper functions.
- Database table verification and sample output display

## Known Limitations
- Still using SQLite
- timestamps and dates still unix-style
- No containerization yet


## Solutions (Later Phases)
- Phase 3: Dockerize entire application + PostgreSQL migration
- Phase 4: Add Hadoop cluster + HDFS storage
- Possibly Orchestrate with Airflow


## Phase 2: Setup Instructions
- Checkout (headless tag) with:
```bash
git checkout phase2
```

### Prerequisites

- Python 3.12.3
- Java 17+ installed and JAVA_HOME configured
- Bash

### Setup

```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### Run
```bash
python src/spark-job.py
```