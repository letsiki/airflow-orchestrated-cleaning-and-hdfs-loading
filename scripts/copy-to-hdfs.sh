#!/bin/bash
# scripts/copy-to-hdfs.sh

# Copy parquet files from local to namenode container
docker cp data/output/. namenode:/tmp/parquet_files/

# Create HDFS directory and copy files
docker exec namenode hdfs dfs -mkdir -p /user/spark/output/
docker exec namenode hdfs dfs -put /tmp/parquet_files/ /user/spark/output/

# Clean up temporary files in container
docker exec namenode rm -rf /tmp/parquet_files/

echo "Parquet files successfully copied to HDFS!"