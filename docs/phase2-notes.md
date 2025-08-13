## Phase 2: Automation Features (Extra 1)
- Add DB initialization script
- Implement retry logic for unmatched subscribers
- Maintain local venv for Spark
- Still using SQLite

## New Features
- Automatic database and table creation if not exists
- Store unmatched transactions for retry in subsequent runs
- Move sql to external files

## Known Limitations
- Still using SQLite (single-threaded, file-based)
- No containerization yet


## Solutions (Later Phases)
- Phase 3: Dockerize entire application + PostgreSQL migration
- Phase 4: Add Hadoop cluster + HDFS storage
- Refactor code into modular components
- Possibly Orchestrate with Airflow

## Choices
- TBC

## Phase 2: Setup Instructions
- Checkout (headless tag) with:
```bash
git checkout phase2
```

### Prerequisites

- Python 3.12.3
- Java 17+ installed and JAVA_HOME configured

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