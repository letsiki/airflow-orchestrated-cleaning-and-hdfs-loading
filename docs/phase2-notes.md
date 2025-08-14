## Phase 2: Automation Features (Extra 1)

### What's New
- Retry logic for unmatched subscribers
- SQL logic to prevent duplicate subscribers
- Modular code structure with external SQL files
- Utility functions for database operations
- Database table verification and output display

### Architecture
- Local Python virtual environment
- PySpark with external SQL files
- SQLite with automated duplicate prevention
- Utility module for reusable functions

### Limitations & Trade-offs
- Still using SQLite (single-threaded, file-based)
- Timestamps and dates remain in unix-style format
- No containerization yet
- Limited retry mechanism (simple file-based storage)

### Setup Instructions
Checkout the phase2 tag:
```bash
git checkout phase2
```

**Prerequisites:**
- Python 3.12.3
- Java 17+ installed and JAVA_HOME configured
- Bash

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
- Implemented anti-join pattern for production-safe incremental loads
- Stored unmatched transactions for retry processing in subsequent runs
- Moved SQL to external files for better maintainability and syntax highlighting
- Created utility functions to reduce code duplication
- Maintained SQLite for consistency while building automation features