from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'test_docker_operator',
    default_args=default_args,
    description='Test DAG for Docker operator functionality',
    schedule=None,  # Manual trigger only
    catchup=False,
    tags=['test', 'docker'],
)

# Test 1: Simple hello world
hello_world_task = DockerOperator(
    task_id='hello_world',
    image='alpine:latest',
    command='echo "Hello World from Docker!"',
    docker_url='tcp://docker:2376',
    tls_ca_cert='/certs/client/ca.pem',
    tls_client_cert='/certs/client/cert.pem',
    tls_client_key='/certs/client/key.pem',
    tls_verify=True,
    auto_remove="force",
    dag=dag,
)

# Test 2: Python script execution
python_task = DockerOperator(
    task_id='python_script',
    image='python:3.12-slim',
    command='python -c "import sys; print(f\'Python version: {sys.version}\'); print(\'Docker operator working!\')"',
    docker_url='tcp://docker:2376',
    tls_ca_cert='/certs/client/ca.pem',
    tls_client_cert='/certs/client/cert.pem',
    tls_client_key='/certs/client/key.pem',
    tls_verify=True,
    auto_remove="force",
    dag=dag,
)

# Test 3: Container with environment variables
env_test_task = DockerOperator(
    task_id='environment_test',
    image='alpine:latest',
    command='sh -c "echo Environment test: $TEST_VAR"',
    environment={'TEST_VAR': 'Docker operator is working!'},
    docker_url='tcp://docker:2376',
    tls_ca_cert='/certs/client/ca.pem',
    tls_client_cert='/certs/client/cert.pem',
    tls_client_key='/certs/client/key.pem',
    tls_verify=True,
    auto_remove="force",
    dag=dag,
)

# Test 4: Network connectivity test
network_test_task = DockerOperator(
    task_id='network_test',
    image='alpine:latest',
    command='sh -c "ping -c 3 google.com && echo Network connectivity working"',
    docker_url='tcp://docker:2376',
    tls_ca_cert='/certs/client/ca.pem',
    tls_client_cert='/certs/client/cert.pem',
    tls_client_key='/certs/client/key.pem',
    tls_verify=True,
    auto_remove="force",
    dag=dag,
)

# Set task dependencies
hello_world_task >> python_task >> env_test_task >> network_test_task