from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.hooks.base import BaseHook  # core; no extra provider
import requests
from docker.types import Mount


@dag(
    dag_id="conn_healthchecks",
    start_date=datetime(2025, 8, 15),
    schedule=None,
    catchup=False,
)
def pipeline():
    @task
    def check_postgres(conn_id: str = "postgres_conn") -> str:
        hook = PostgresHook(postgres_conn_id=conn_id)
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                cur.fetchone()
        return "✅ Postgres connection OK"

    @task
    def check_hdfs_webhdfs(conn_id: str = "hdfs_conn", path: str = "/") -> str:
        """Use an Airflow HTTP connection to hit WebHDFS LISTSTATUS."""
        c = BaseHook.get_connection(conn_id)
        scheme = c.schema or "http"
        host = c.host
        port = f":{c.port}" if c.port else ""
        extras = c.extra_dejson or {}
        webhdfs_path = extras.get("webhdfs_path", "/webhdfs/v1")
        user = extras.get("user", "hdfs")

        list_path = "" if path == "/" else path.lstrip("/")
        url = f"{scheme}://{host}{port}{webhdfs_path}/{list_path}?op=LISTSTATUS&user.name={user}"
        r = requests.get(url, timeout=5)
        r.raise_for_status()
        n = len(r.json().get("FileStatuses", {}).get("FileStatus", []))
        return f"✅ HDFS (WebHDFS) OK — {n} items under '{path}'"

    @task
    def format_ds_with_run_suffix(**context):
        """
        Format the ds (execution date) with _a or _b suffix based on the hour.
        For runs at 00:00 -> _a, for runs at 12:00 -> _b
        """
        execution_date = context["ds"]
        logical_date = context["logical_date"]

        # Get the hour from the logical_date to determine if it's first or second run of the day
        hour = logical_date.hour

        if hour < 12:
            suffix = "_a"
        else:
            suffix = "_b"

        formatted_ds = f"{execution_date}{suffix}"
        print(f"Formatted ds: {formatted_ds}")
        return formatted_ds

    build_db_init_image = DockerOperator(
        task_id="build_db_init_image",
        image="docker:27-cli",
        mount_tmp_dir=False,
        command="docker build -f /workspace/Dockerfile.db-init -t optasia-db-init:latest /workspace",
        docker_url="tcp://dind:2375",  # <-- IMPORTANT (or use docker_url="tcp://docker:2375")
        auto_remove="force",
        environment={
            "DOCKER_HOST": "unix:///var/run/docker.sock"
        },  # CLI -> daemon via unix socket
        mounts=[
            # unix socket from DinD host into this CLI container
            Mount(
                type="bind",
                source="/var/run/docker.sock",
                target="/var/run/docker.sock",
            ),
            # your build context (already mounted into DinD at /workspace)
            Mount(
                type="bind", source="/workspace", target="/workspace", read_only=True
            ),
        ],
    )
    # Use host network mode since optasia_db network is not visible in DinD
    run_db_init = DockerOperator(
        task_id="run_db_init",
        image="optasia-db-init:latest",
        docker_url="tcp://dind:2375",
        mount_tmp_dir=False,
        network_mode="host",  # Use host network mode
        auto_remove="force",
        environment={
            "PGHOST": "postgres",
            "PGPORT": "5432",
            "PGDATABASE": "optasia",
            "PGUSER": "testuser",
            "PGPASSWORD": "password",
        },
    )
    debug_networks = DockerOperator(
        task_id="debug_networks",
        image="docker:27-cli",
        docker_url="tcp://dind:2375",
        mount_tmp_dir=False,
        command="sh -c 'DOCKER_HOST=tcp://host.docker.internal:2375 docker network ls || DOCKER_HOST=tcp://172.17.0.1:2375 docker network ls || echo \"Could not connect to Docker daemon\"'",
        auto_remove="force",
    )
    (
        check_postgres()
        >> debug_networks
        >> check_hdfs_webhdfs()
        >> format_ds_with_run_suffix()
        >> build_db_init_image
        >> run_db_init
    )


dag = pipeline()
