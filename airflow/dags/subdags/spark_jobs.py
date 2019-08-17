from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import LivySparkOperator


def spark_jobs_subdag(
    parent_dag_id,
    dag_id,
    http_conn_id,
    session_kind,
    *args, **kwargs):
    """Returns the SubDAG for processing data with Spark"""

    dag = DAG(
        f"{parent_dag_id}.{dag_id}",
        **kwargs
    )

    start_operator = DummyOperator(dag=dag, task_id='start_operator')
    end_operator = DummyOperator(dag=dag, task_id='end_operator')

    def create_task(script_name):
        """Returns an operator that executes the Spark script under the passed name"""

        with open(f'/Users/olegpolakow/airflow/dags/scripts/{script_name}.py', 'r') as f:
            spark_script = f.read()

        return LivySparkOperator(
            dag=dag,
            task_id=f"{script_name}_script",
            spark_script=spark_script,
            http_conn_id=http_conn_id,
            session_kind=session_kind)

    business_hours = create_task("business_hours")
    business_attributes = create_task("business_attributes")
    cities = create_task("cities")
    addresses = create_task("addresses")
    business_categories = create_task("business_categories")
    businesses = create_task("businesses")
    reviews = create_task("reviews")
    users = create_task("users")
    elite_years = create_task("elite_years")
    friends = create_task("friends")
    checkins = create_task("checkins")
    tips = create_task("tips")
    photos = create_task("photos")
    city_weather = create_task("city_weather")

    # Specify relationships between operators
    start_operator >> cities >> addresses >> businesses >> end_operator
    start_operator >> cities >> city_weather >> end_operator
    start_operator >> business_hours >> end_operator
    start_operator >> business_attributes >> end_operator
    start_operator >> business_categories >> end_operator
    start_operator >> reviews >> end_operator
    start_operator >> users >> end_operator
    start_operator >> elite_years >> end_operator
    start_operator >> friends >> end_operator
    start_operator >> checkins >> end_operator
    start_operator >> tips >> end_operator
    start_operator >> photos >> end_operator

    return dag
