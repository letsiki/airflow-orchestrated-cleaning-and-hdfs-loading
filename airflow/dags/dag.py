from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.standard.operators.python import PythonOperator

# Default arguments for the DAG
default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def format_ds_with_run_suffix(**context):
    """
    Format the ds (execution date) with _a or _b suffix based on the hour.
    For runs at 00:00 -> _a, for runs at 12:00 -> _b
    """
    execution_date = context['ds']
    logical_date = context['logical_date']
    
    # Get the hour from the logical_date to determine if it's first or second run of the day
    hour = logical_date.hour
    
    if hour < 12:
        suffix = '_a'
    else:
        suffix = '_b'
    
    formatted_ds = f"{execution_date}{suffix}"
    print(f"Formatted ds: {formatted_ds}")
    return formatted_ds

# Create the DAG
dag = DAG(
    'optasia_etl_pipeline',
    default_args=default_args,
    description='ETL Pipeline running every 12 hours with Docker containers',
    schedule='0 */12 * * *',  # Every 12 hours at 00:00 and 12:00
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'docker', 'optasia'],
)

# Task 1: Check PostgreSQL health
postgres_health_check = HttpSensor(
    task_id='check_postgres_health',
    http_conn_id='postgres_health',  # You'll need to configure this connection in Airflow
    endpoint='',
    request_params={},
    timeout=60,
    poke_interval=10,
    retries=3,
    dag=dag,
)

# # Alternative health check using Docker if HTTP sensor doesn't work
# postgres_health_check_docker = DockerOperator(
#     task_id='check_postgres_health_docker',
#     image='postgres:latest',
#     command='pg_isready -h postgres -p 5432 -U testuser',
#     docker_url='tcp://docker:2376',
#     tls_ca_cert='/certs/client/ca.pem',
#     tls_client_cert='/certs/client/cert.pem',
#     tls_client_key='/certs/client/key.pem',
#     tls_verify=True,
#     auto_remove="force",
#     network_mode='bridge',  # Adjust based on your network setup
#     dag=dag,
# )

# Task 2: Check HDFS Namenode health
hdfs_health_check = HttpSensor(
    task_id='check_hdfs_health',
    http_conn_id='hdfs_health',  # You'll need to configure this connection in Airflow
    endpoint='',
    timeout=60,
    poke_interval=10,
    retries=3,
    dag=dag,
)

# # Alternative HDFS health check using Docker
# hdfs_health_check_docker = DockerOperator(
#     task_id='check_hdfs_health_docker',
#     image='alpine:latest',
#     command='sh -c "wget -q --spider http://namenode:9870/ && echo HDFS is healthy"',
#     docker_url='tcp://docker:2376',
#     tls_ca_cert='/certs/client/ca.pem',
#     tls_client_cert='/certs/client/cert.pem',
#     tls_client_key='/certs/client/key.pem',
#     tls_verify=True,
#     auto_remove="force",
#     network_mode='bridge',  # Adjust based on your network setup
#     dag=dag,
# )

# Task 3: Format the ds parameter
format_ds_task = PythonOperator(
    task_id='format_ds_parameter',
    python_callable=format_ds_with_run_suffix,
    dag=dag,
)

# Task 4: Database initialization
db_init_task = DockerOperator(
    task_id='db_initialization',
    image='optasia-db-init:latest',  # Your built image
    command='python src/db_init.py',
    docker_url='tcp://docker:2376',
    tls_ca_cert='/certs/client/ca.pem',
    tls_client_cert='/certs/client/cert.pem',
    tls_client_key='/certs/client/key.pem',
    tls_verify=True,
    auto_remove="force",
    network_mode='bridge',  # Adjust to match your infrastructure network
    # Mount volumes if needed for your setup
    # volumes=['./data:/app/data:ro'],  # Uncomment and adjust if you need to mount data
    dag=dag,
)

# Task 5: Spark ETL job
spark_etl_task = DockerOperator(
    task_id='spark_etl_job',
    image='optasia-spark-job:latest',  # Your built image
    command='python3 src/spark_job.py',
    docker_url='tcp://docker:2376',
    tls_ca_cert='/certs/client/ca.pem',
    tls_client_cert='/certs/client/cert.pem',
    tls_client_key='/certs/client/key.pem',
    tls_verify=True,
    auto_remove="force",
    network_mode='bridge',  # Adjust to match your infrastructure network
    environment={
        'ds': "{{ task_instance.xcom_pull(task_ids='format_ds_parameter') }}"
    },
    # Mount volumes if needed for your setup
    # volumes=[
    #     './data:/app/data',  # Input/output data
    #     './jars:/app/jars:ro',  # JDBC drivers
    # ],
    dag=dag,
)

# Define task dependencies
# Option 1: Using HTTP sensors (preferred if you can configure the connections)
postgres_health_check >> hdfs_health_check >> format_ds_task >> db_init_task >> spark_etl_task

# Option 2: Using Docker health checks (if HTTP sensors are not feasible)
# postgres_health_check_docker >> hdfs_health_check_docker >> format_ds_task >> db_init_task >> spark_etl_task

# If you want to use both health check methods as alternatives:
# [postgres_health_check, postgres_health_check_docker] >> [hdfs_health_check, hdfs_health_check_docker] >> format_ds_task >> db_init_task >> spark_etl_task