## Phase 1: Core Implementation

### What's New
- Basic Spark ETL pipeline implementation
- CSV data cleaning and transformation
- SQLite database integration
- Parquet file output

### Architecture
- Local Python virtual environment
- PySpark for data processing
- SQLite for subscriber storage
- Local file system for input/output

### Limitations & Trade-offs
- No database initialization script
- All code in single file
- SQLite stores dates in unix-like format
- Manual setup required
- No retry logic for failed transactions

### Setup Instructions
Checkout the phase1 tag:
```bash
git switch main
git checkout phase1
```

**Prerequisites:**
- Python 3.12.3
- Java 17+ installed and JAVA_HOME configured

**Setup:**
```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

**Run:**
```bash
python src/spark-job.py
```

### Design Decisions
- Chose SQL over DataFrame API for better readability and natural data querying
- Disabled automatic schema inference to prevent sub_id being misinterpreted as integer
- Used SQLite for simplicity in initial implementation
- Focused on core functionality over production features