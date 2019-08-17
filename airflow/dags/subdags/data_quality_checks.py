# from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import RedshiftValueCheckOperator


def data_quality_checks_subdag(
    parent_dag_id,
    dag_id,
    redshift_conn_id,
    check_definitions,
    *args, **kwargs):
    """Returns the SubDAG for performing data quality checks"""

    dag = DAG(
        f"{parent_dag_id}.{dag_id}",
        **kwargs
    )

    start_operator = DummyOperator(dag=dag, task_id='start_operator')
    end_operator = DummyOperator(dag=dag, task_id='end_operator')

    for check in check_definitions:
        check_operator = RedshiftValueCheckOperator(
            dag=dag, 
            task_id=check.get('task_id', None), 
            redshift_conn_id="redshift",
            sql=check.get('sql', None), 
            pass_value=check.get('pass_value', None),
            tolerance=check.get('tolerance', None)
        )

        start_operator >> check_operator >> end_operator

    return dag