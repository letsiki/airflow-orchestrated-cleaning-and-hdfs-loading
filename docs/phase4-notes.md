## Phase 4: Hadoop Cluster & HDFS (Extra 3)

### What's New
- Hadoop cluster with HDFS integration
- Manual parquet file transfer to HDFS via bash script
- NameNode and DataNode containerization
- HDFS web UI for file management
- Bash script for HDFS operations (reusable in Airflow)

### Architecture
- Docker Compose orchestrating full stack
- Hadoop cluster (NameNode + DataNodes)
- PostgreSQL database
- Spark application writing to local output
- Manual HDFS transfer via bash script

### Limitations & Trade-offs
- Single-node Hadoop cluster (pseudo-distributed)
- No automatic cluster scaling
- Still requires manual container execution
- No data replication policies configured
- Limited HDFS optimization for small datasets

**Prerequisites:**
- Docker and Docker Compose installed
- At least 8GB available RAM

**Setup & Run:**
```bash
# Start all services (PostgreSQL + Hadoop cluster)
docker compose up -d

# Wait for HDFS to initialize (check NameNode UI: http://localhost:9870)

# Build application containers
docker build -t db-init -f Dockerfile.db-init .
docker build -t spark-job -f Dockerfile.spark-job .

# Run database initialization
docker run --rm --network=optasia_db db-init

# Run Spark job (output to local directory)
docker run --rm --network=optasia_db \
  -v $(pwd)/data/input:/app/data/input \
  -v $(pwd)/data/output:/app/data/output \
  spark-job

# Transfer parquet files to HDFS
./scripts/copy-to-hdfs.sh
```

**Verify HDFS Output:**
- NameNode Web UI: http://localhost:9870
- DataNode Web UI: http://localhost:9864
- Browse HDFS files at: `/user/spark/output/`

### Design Decisions
- Implemented pseudo-distributed Hadoop for development/demo purposes
- Kept Spark job unchanged from Phase 3 for simplicity
- Created reusable bash script for HDFS operations (copy-to-hdfs.sh)
- Used Docker volumes for HDFS persistence across container restarts
- Separated data processing from storage operations for cleaner workflow
- Maintained separate containers for future Airflow orchestration compatibility