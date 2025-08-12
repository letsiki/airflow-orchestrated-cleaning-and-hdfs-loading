## Phase 1: Simple Setup
- Local venv for Spark
- No containerization  
- SQLite for simplicity

## Known Limitations
- No DB init script
- Spark overwrites tables (not production-safe for incremental data)

## Solutions (Later Phases)
- DB init script when we migrate to PostgreSQL
- Anti-join to exclude existing subscribers and use append mode instead of overwrite

## Phase 1: Setup Instructions
- Checkout (Headless) to Phase 1 commit

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