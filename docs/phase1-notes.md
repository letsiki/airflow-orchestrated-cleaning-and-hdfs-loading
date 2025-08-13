## Phase 1: Simple Setup
- Local venv for Spark
- No containerization  
- SQLite for simplicity

## Known Limitations
- No DB init script
- Harder reproducibility without containerization.
- All code in one file
- sqlite stores date and timestamp in unix-like format (ms)

## Solutions (Later Phases)
- DB init script when we migrate to PostgreSQL
- Make code, more modular (move sql to external files, create classes-functions)
- We can cast dates and timestamps to formatted strings but that would affect future phases where we use PostgreSQL. I will leave as it, it will fix itself in the following phases.

## Choices
- Even though, I would consider myself more comfortable in Python, than in SQL, I generally prefer SQL when it comes to Spark.
- The reason is that it feels a lot more natural to me, when it comes to querying data and I prefer it over the chained python methods.
- In terms of performance, my understanding is that they are both similar and both slower than using native Scala. 
- I chose not too let Spark automatically infer the schema of the csv files. 
- My thought process was that both tables have the *sub_id* column, which would probably get misinterpreted as an integer.
- See sql notes for cleaning-thought process

## Phase 1: Setup Instructions
- Checkout (headless tag) with:
```bash
git checkout phase1
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