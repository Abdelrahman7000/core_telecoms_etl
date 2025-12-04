"""
### Run a dbt Core project as a task group with Cosmos

Simple DAG showing how to run a dbt project as a task group, using
an Airflow connection and injecting a variable into the dbt project.
"""

from airflow.sdk import dag, chain
#from airflow.sdk.decorators import task,dag
from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig

# adjust for other database types
from cosmos.profiles.postgres import PostgresUserPasswordProfileMapping
import os
from include.tasks import ingest_website_forms,ingest_call_center_logs,ingest_social_media_data,staging_static_data
from include.load import load_data_to_pg
from include.transformation import transform_call_centers_logs,transform_media_complaints,transform_web_forms

YOUR_NAME = "YOUR_NAME"
CONNECTION_ID = "db_conn"
DB_NAME = "postgres"
SCHEMA_NAME = "postgres"
MODEL_TO_QUERY = "model2"
# The path to the dbt project
DBT_PROJECT_PATH = f"{os.environ['AIRFLOW_HOME']}/include/dbt/my_dbt_project"
DBT_EXECUTABLE_PATH = f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"

# OPTIONAL: The path where Cosmos will find the dbt executable
# in the virtual environment created in the Dockerfile if you cannot 
# install your dbt adapter in requirements.txt due to package conflicts.
# DBT_EXECUTABLE_PATH = f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id=CONNECTION_ID,
        profile_args={"schema": SCHEMA_NAME},
    ),
)

execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH,
)


@dag(
    params={"my_name": YOUR_NAME},
)
def my_simple_dbt_dag():
    transform_data = DbtTaskGroup(
        group_id="transform_data",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=execution_config,
        operator_args={
            "vars": '{"my_name": {{ params.my_name }} }',
        },
        default_args={"retries": 2},
    )

    
    # Define ingestion tasks
    @task 
    def ingest_data_tasks():
        ingest_website_forms()
        ingest_call_center_logs()
        ingest_social_media_data()
        staging_static_data("customers_dataset", source="s3")
        staging_static_data("agents_dataset", source="url")
    
    @task
    def load_data_tasks():
        load_data_to_pg(
            table_name="call_center_logs",
            mode="incremental",
            source_prefix="raw/call_center_logs/",
            transform_fn=transform_call_centers_logs
        )
        load_data_to_pg(
            table_name="media_complaints",
            mode="incremental",
            source_prefix="raw/social_media/",
            transform_fn=transform_media_complaints
        )
        load_data_to_pg(
            table_name="web_forms",
            mode="incremental",
            source_prefix="raw/web_forms/",
            transform_fn=transform_web_forms
        )
        
        
    ingestion_task = ingest_data_tasks()
    load_task = load_data_tasks()

    ingestion_task >> load_task >> transform_data
    

my_simple_dbt_dag()
