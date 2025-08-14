## Phase 3: Dockerization & PostgreSQL (Extra 2)
- Dockerize the entire application
- Replace SQLite with PostgreSQL
- Add docker-compose for orchestration
- Maintain all Phase 2 automation features

## New Features
- Full containerization with Docker
- PostgreSQL database
- Docker-compose setup for easy deployment
- Proper database initialization with PostgreSQL


## Known Limitations
- Still storing parquet files locally (not in distributed storage)
- Single-node setup
- No Orchestration-Scheduling

## Solutions (Later Phases)
- Phase 4: Add Hadoop cluster + HDFS storage for parquet files
- Multi-node cluster deployment
- Possibly orchestrate with Airflow

## Phase 3: Setup Instructions
Checkout the phase3 tag:
```bash
git checkout phase3
```

### Prerequisites
- Docker and Docker Compose installed
- At least 8GB available RAM

### Setup and Run
```bash
# Start all services
docker-compose up -d

# Check logs
docker-compose logs -f spark-app
```
