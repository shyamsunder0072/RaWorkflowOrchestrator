from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators import CoutureSparkOperator, CouturePySparkOperator, CoutureDaskYarnOperator
from airflow.operators.dag_operator import SkippableDagOperator, DagOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.example_dags.subdags.subdag import subdag
from airflow.operators.subdag_operator import SubDagOperator

appName = 'CoutureExample'


# args = {
#     'owner': 'couture',
#     'start_date': datetime(2019, 4, 15),
#     'depends_on_past': False,
# }

# schedule = None
# dag = DAG('CoutureExample', default_args=args, schedule_interval=schedule)
# CheckFeatures = CoutureSparkOperator(
#     task_id='CheckFeatures',
#     app_name=appName,
#     class_path='org.apache.spark.examples.SparkPi',
#     code_artifact='spark-examples_2.11-2.3.1.jar',
#     application_arguments=[],
#     dag=dag,
#     description=''
# )
