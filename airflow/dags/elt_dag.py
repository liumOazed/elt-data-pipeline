from datetime import datetime, timedelta
from airflow import DAG
from docker.types import Mount ## because u r orchestrating docker services or docker containers u need a specific docker type comes form airflow to actually orchestrate them
from airflow.operators.python import PythonOperator ## crucial because our elt_cript is a python script
from airflow.operators.bash import BashOperator ## crucial because we need to execute a bash command
from airflow.providers.docker.operators.docker import DockerOperator
import subprocess 

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

def run_elt_script():
    script_path = '/opt/airflow/elt_script/elt_script.py'# we pointed directly to the path of the elt script in the docker container
    result = subprocess.run(['python', script_path], capture_output=True, text=True)
    # control structure
    if result.returncode != 0:
        raise Exception(f"ELT script failed with error {result.stderr}")
    else:
        print(result.stdout)
        
# Now lets puth these in to the dag

dag = DAG(
    'elt_and_dbt', # name of the dag
    default_args=default_args,
    description='An ELT Workflow with DBT',
    start_date = datetime(2024, 12, 15),
    catchup = False,
)

# Now its time to write our task
# 1. Gave identifier to the task
# 2. Added the python function inorder to run the elt script
# 3. Attached it to the dag we created
t1 = PythonOperator(
    task_id='run_elt_script',
    python_callable=run_elt_script,
    dag=dag,
)

# task 2 and its dbt

t2 = DockerOperator(
    task_id='dbt_run',
    image='ghcr.io/dbt-labs/dbt-postgres:1.4.7',
    command=[
        "run",
        "--profiles-dir",
        "/root",
        "--project-dir",
        "/dbt",
        "--full-refresh"
    ],
    auto_remove=True,
    docker_url="unix://var/run/docker.sock",
    network_mode="elt-data-pipeline_elt_network",
    mounts=[
        Mount(
            source='/home/zed/elt-data-pipeline/custom_postgres',
            target='/dbt',
            type='bind'
        ),
         Mount(
            source='/home/zed/.dbt',
            target='/root',
            type='bind'
        ),      
    ],
    dag=dag
)

# Create the order in which tasks will run
t1 >> t2