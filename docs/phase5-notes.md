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
- At least 16GB available RAM
- Basic understanding of Airflow concepts

**Setup & Run:**
```bash
# Start infrastructure services (PostgreSQL + Hadoop cluster)
docker compose docker compose up -d

# Start Airflow services (connects to existing infrastructure)
cd airflow
docker compose -f docker compose up -d
cd ..

# Access Airflow Web UI: http://localhost:8080

# Choose optasia_etl_pipeline and enable

# You can click on it and press trigger for a sample dag run

# You can find the parquet file(s) at NameNode Web UI: http://localhost:9870
```

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