from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.postgres_operator import PostgresOperator
from subdags.s3_to_redshift import s3_to_redshift_subdag
from subdags.data_quality_checks import data_quality_checks_subdag

from datetime import datetime, timedelta
import os
import yaml

start_date = datetime.now() - timedelta(days=2)

default_args = {
    'owner': "polakowo",
    'start_date': start_date,
    'catchup': False,
    'depends_on_past': False,
    'retries': 0
}

DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")

dag = DAG(DAG_ID,
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval=None,
          max_active_runs=1)

start_operator = DummyOperator(dag=dag, task_id='start_operator')

# Read table definitions from YAML file
with open('/Users/olegpolakow/airflow/dags/configs/table_definitions.yml', 'r') as f:
    table_definitions = yaml.safe_load(f)

# Create the SubDAG for copying S3 tables into Redshift
subdag_id = "copy_data_to_redshift"
copy_data_to_redshift = SubDagOperator(
    subdag=s3_to_redshift_subdag(
        parent_dag_id=DAG_ID,
        dag_id=subdag_id,
        table_definitions=table_definitions,
        redshift_conn_id='redshift',
        redshift_schema='public',
        s3_conn_id='aws_credentials',
        s3_bucket='polakowo-yelp2/staging_data',
        load_type='rebuild',
        schema_location='Local',
        start_date=start_date),
    task_id=subdag_id,
    dag=dag)

# Read check definitions from YAML file
with open('/Users/olegpolakow/airflow/dags/configs/check_definitions.yml', 'r') as f:
    check_definitions = yaml.safe_load(f)

# Create the SubDAG for performing data quality checks
subdag_id = "data_quality_checks"
data_quality_checks = SubDagOperator(
    subdag=data_quality_checks_subdag(
        parent_dag_id=DAG_ID,
        dag_id=subdag_id,
        redshift_conn_id='redshift',
        check_definitions=check_definitions, 
        start_date=start_date),
    task_id=subdag_id,
    dag=dag)

end_operator = DummyOperator(dag=dag, task_id='end_operator')

# Specify relationships between operators
start_operator >> copy_data_to_redshift >> data_quality_checks >> end_operator
