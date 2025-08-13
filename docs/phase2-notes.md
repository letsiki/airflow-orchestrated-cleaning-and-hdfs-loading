## Phase 2: Automation Features (Extra 1)
- Add DB initialization script
- Implement retry logic for unmatched subscribers
- Maintain local venv for Spark
- Still using SQLite

## New Features
- Automatic database and table creation if not exists
- Store unmatched transactions for retry in subsequent runs
- Anti-join logic to prevent duplicate subscribers
- Append mode instead of overwrite for incremental processing

## Known Limitations
- Still using SQLite (single-threaded, file-based)
- No containerization yet
- All code still in one file
- Limited retry mechanism (simple file-based storage)

## Solutions (Later Phases)
- Phase 3: Dockerize entire application + PostgreSQL migration
- Phase 4: Add Hadoop cluster + HDFS storage
- Refactor code into modular components
- Implement more sophisticated retry mechanisms with proper queueing

## Choices
- Store unmatched transactions as parquet files for retry processing
- Use `mode("ignore")` combined with anti-join for safe incremental loads
- Keep retry logic simple - check for previously unmatched transactions on each run
- DB init will create tables with proper constraints and indexes

## Phase 2: Setup Instructions
- Checkout (headless tag) with:
```bash
git checkout <tag_name>
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

### Verify

- Database auto-created with proper schema
- Unmatched transactions stored in data/retry/ folder
- Subsequent runs process retry data