from airflow.plugins_manager import AirflowPlugin
from spark_plugin.operators.spark_operator import SparkSubmitOperator, LivySparkOperator


class S3ToRedshiftPlugin(AirflowPlugin):
    name = "S3ToRedshiftPlugin"
    operators = [
        SparkSubmitOperator,
        LivySparkOperator
    ]
    # Leave in for explicitness
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
