## Phase 5: Airflow Orchestration (Theoretical)

### What's New
- Apache Airflow for workflow orchestration
- DAG-based pipeline scheduling
- DockerOperator integration for Spark jobs
- Automated retry and error handling
- Web UI for pipeline monitoring and management

### Architecture
- Separate docker-compose files for infrastructure vs orchestration
- Infrastructure: PostgreSQL + Hadoop cluster (shared with Phase 4)
- Airflow: Web server, scheduler, and workers
- Shared Docker networks for service communication
- Multi-step DAG: Spark processing → HDFS storage → cleanup

### Limitations & Trade-offs
- Increased complexity in setup and maintenance
- Additional resource requirements for Airflow services
- Learning curve for DAG development
- Overkill for simple batch jobs
- Requires monitoring and alerting setup

**Prerequisites:**
- Docker and Docker Compose installed
- At least 8GB available RAM
- Basic understanding of Airflow concepts

**Setup & Run:**
```bash
# Start infrastructure services (PostgreSQL + Hadoop cluster)
docker compose up -d

# Start Airflow services (connects to existing infrastructure)
cd airflow
docker compose up -d

# Access Airflow Web UI: http://localhost:8080 (might have to wait for 1-minute max)

# Enable optasia_pipeline (toggle)

# Click on it and then navigate to 'Runs'

# Choose active run and watch its progress

# Once the dag run is complete...

# You can find the parquet file(s) at NameNode Web UI: http://localhost:9870

# And also locally, at data/output

```

## Scheduling & File Names
**The DAG will run twice per day because data_transactions.csv  contains 12-hour data.**
**The generated parquet file will have a prefix containing the following:**
    - dag run date
    - '_a_' if it was the first half of the day
    - '_b_' if it was the second half of the day

**Pipeline Monitoring:**
- Airflow Web UI: http://localhost:8080
- NameNode Web UI: http://localhost:9870
- Monitor DAG runs, task logs, and data lineage

### Design Decisions
- Separated infrastructure (docker-compose.infrastructure.yml) from orchestration (docker-compose.airflow.yml)
- Used DockerOperator for Spark job execution, BashOperator for HDFS file operations
- Implemented multi-step DAG: db-init → spark-processing → hdfs-storage → cleanup
- Configured automatic retries for failed database connections
- Added data quality checks between pipeline stages
- Used shared Docker networks to connect Airflow with existing infrastructure
- Implemented email alerts for pipeline failures (configuration required)
- Used Airflow Variables for environment-specific configurations