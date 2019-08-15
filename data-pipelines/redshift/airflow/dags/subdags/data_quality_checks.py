# from datetime import datetime, timedelta
from airflow import DAG
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

    for check in check_definitions:
        RedshiftValueCheckOperator(
            dag=dag, 
            task_id=check.get('task_id', None), 
            redshift_conn_id="redshift",
            sql=check.get('sql', None), 
            pass_value=check.get('pass_value', None),
            tolerance=check.get('tolerance', None)
        )

    return dag