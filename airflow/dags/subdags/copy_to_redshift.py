from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import S3ToRedshiftOperator

def copy_to_redshift_subdag(
    parent_dag_id,
    dag_id,
    table_definitions,
    redshift_conn_id,
    redshift_schema,
    s3_conn_id,
    s3_bucket,
    load_type,
    schema_location,
    *args, **kwargs):
    """Returns the SubDAG for copying S3 tables into Redshift"""

    dag = DAG(
        f"{parent_dag_id}.{dag_id}",
        **kwargs
    )

    start_operator = DummyOperator(dag=dag, task_id='start_operator')
    end_operator = DummyOperator(dag=dag, task_id='end_operator')

    def get_table(table_name):
        """Returns the table under the passed name"""

        for table in table_definitions:
            if table.get('table_name', None) == table_name:
                return table

    def create_task(table):
        """Returns an operator for copying the table into Redshift"""
        
        return S3ToRedshiftOperator(
            dag=dag,
            task_id=f"copy_{table.get('table_name', None)}_to_redshift",
            redshift_conn_id=redshift_conn_id,
            redshift_schema=redshift_schema,
            table=table.get('table_name', None),
            s3_conn_id=s3_conn_id,
            s3_bucket=s3_bucket,
            s3_key=table.get('s3_key', None),
            load_type=load_type,
            copy_params=table.get('copy_params', None),
            origin_schema=table.get('origin_schema', None),
            primary_key=table.get('primary_key', None),
            foreign_key=table.get('foreign_key', {}),
            schema_location=schema_location)

    businesses = create_task(get_table("businesses"))
    business_attributes = create_task(get_table("business_attributes"))
    categories = create_task(get_table("categories"))
    business_categories = create_task(get_table("business_categories"))
    addresses = create_task(get_table("addresses"))
    cities = create_task(get_table("cities"))
    city_weather = create_task(get_table("city_weather"))
    business_hours = create_task(get_table("business_hours"))
    reviews = create_task(get_table("reviews"))
    users = create_task(get_table("users"))
    elite_years = create_task(get_table("elite_years"))
    friends = create_task(get_table("friends"))
    checkins = create_task(get_table("checkins"))
    tips = create_task(get_table("tips"))
    photos = create_task(get_table("photos"))

    # We could execute the entire YAML file in parallel
    # But let's respect the referential integrity
    # Look at the UML diagram to build the acyclic graph of references

    start_operator >> cities
    start_operator >> categories
    start_operator >> users
    
    cities >> addresses
    cities >> city_weather >> end_operator

    addresses >> businesses

    businesses >> business_attributes >> end_operator
    businesses >> business_categories >> end_operator
    businesses >> business_hours >> end_operator
    businesses >> checkins >> end_operator
    businesses >> photos >> end_operator
    businesses >> tips >> end_operator
    businesses >> reviews >> end_operator

    categories >> business_categories >> end_operator

    users >> reviews >> end_operator
    users >> tips >> end_operator
    users >> friends >> end_operator
    users >> elite_years >> end_operator

    return dag
