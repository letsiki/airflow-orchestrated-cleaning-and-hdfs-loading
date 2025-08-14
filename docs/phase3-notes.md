## Phase 3: Dockerization & PostgreSQL (Extra 2)

### What's New
- Full application containerization
- PostgreSQL database replacement
- Docker Compose for database orchestration
- Separate containers for initialization and processing
- Production-ready database with proper data types

### Architecture
- Docker containers for all components
- PostgreSQL database with Docker Compose
- Separate db-init and spark-job containers
- Manual container execution (Airflow-compatible)
- Bind mounts for data persistence

### Limitations & Trade-offs
- Still storing parquet files locally (not in distributed storage)
- Single-node setup
- No orchestration or scheduling
- Manual container execution required

### Setup Instructions
Checkout the phase3 tag:
```bash
git checkout phase3
```

**Prerequisites:**
- Docker and Docker Compose installed
- At least 8GB available RAM

**Setup & Run:**
```bash
# Start PostgreSQL
docker compose up -d

# Build the containers
docker build -t db-init -f Dockerfile.db-init .
docker build -t spark-job -f Dockerfile.spark-job .

# Run database initialization
docker run --rm --network=optasia_db db-init

# Run Spark job
docker run --rm --network=optasia_db \
  -v $(pwd)/data/input:/app/data/input \
  -v $(pwd)/data/output:/app/data/output \
  spark-job
```

### Design Decisions
- Separated containers for initialization vs processing to support future Airflow orchestration
- Used PostgreSQL for better production compatibility and data type handling
- Maintained anti-join logic instead of PostgreSQL UPSERT for database portability
- Implemented bind mounts for data access while keeping containers stateless
- Used explicit Docker network naming for consistent container communication