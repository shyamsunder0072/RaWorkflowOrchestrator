# flake8: noqa
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators import SparkOperator, PySparkOperator, CoutureDaskYarnOperator
from airflow.operators.dag_operator import SkippableDagOperator, DagOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators import CoutureJupyterOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators import PythonOperator
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

appName = 'CoutureExample'

default_args = {
    'owner': 'couture',
    'depends_on_past': False,
    'start_date': datetime(2019, 4, 15),
    'retries': 0,
}

schedule = None
dag = DAG('CoutureExample', default_args=default_args, catchup=False, schedule_interval=schedule)
#
# LoadData = SparkOperator(
#     task_id='LoadData',
#     app_name=appName,
#     class_path='org.apache.spark.examples.SparkPi',
#     code_artifact='spark-examples_2.11-2.3.1.jar',
#     application_arguments=[],
#     dag=dag,
#     description='This task was inserted from the code bricks available on from Developer -> Manage Dags. The task name have been updated according to the scenario'
# )
#
# StatsGeneration = PySparkOperator(
#     task_id='StatsGeneration',
#     app_name=appName,
#     code_artifact='pi.py',
#     application_arguments=[],
#     dag=dag,
#     description='Stats Generation'
# )
#
# StatsGeneration.set_upstream(LoadData)
#
# ModelTraining = DagOperator(
#     task_id='ModelTraining',
#     run_dag_id='ExampleModelTraining',
#     dag=dag,
#     description='Model Training'
# )
#
# ModelTraining.set_upstream(StatsGeneration)
#
# ModelSelection = SkippableDagOperator(
#     task_id='ModelSelection',
#     run_dag_id='ExampleModelSelection',
#     skip_dag=True,
#     dag=dag,
#     description='Model Selection'
# )
# ModelSelection.set_upstream([ModelTraining])
#
# TriggerUseCaseDag = TriggerDagRunOperator(
#     task_id='TriggerUseCaseDag',
#     trigger_dag_id="ExampleFeatureValidation",
#     dag=dag
# )
# TriggerUseCaseDag.set_upstream(ModelSelection)
#
# ExecuteUseCaseNotebook = CoutureJupyterOperator(
#     task_id='ExecuteUseCaseNotebook',
#     input_notebook='clusterOptimization.ipynb',
#     output_notebook='clusterOptimization.ipynb',
#     parameters={},
#     export_html=True,
#     output_html='',
#     dag=dag,
#     description='Execute jupyter notebook from workflow task'
# )
#
# ExecuteUseCaseNotebook.set_upstream(ModelSelection)
#
# simpleDaskJob = CoutureDaskYarnOperator(
#     task_id='simpleDaskJob',
#     app_name='MyDaskJob',
#     code_artifact='simple.py',
#     dag=dag,
#     description=''
# )
#
# multivariate_visualisation = BashOperator(
#     task_id='multivariate_visualisation',
#     depends_on_past=False,
#     bash_command='python multivariate_visualisation.py',
#     dag=edag
# )
#
# def print_context(ds, **kwargs):
#     pprint(kwargs)
#     print(ds)
#     return 'Whatever you return gets printed in the logs'
#
#
# run_this = PythonOperator(
#     task_id='print_the_context',
#     provide_context=True,
#     python_callable=print_context,
#     dag=dag,
# )
#
# from airflow.contrib.kubernetes.secret import Secret
# from airflow.contrib.kubernetes.volume import Volume
# from airflow.contrib.kubernetes.volume_mount import VolumeMount
# from airflow.contrib.kubernetes.pod import Port
#
#
# secret_file = Secret('volume', '/etc/sql_conn', 'airflow-secrets', 'sql_alchemy_conn')
# secret_env  = Secret('env', 'SQL_CONN', 'airflow-secrets', 'sql_alchemy_conn')
# secret_all_keys  = Secret('env', None, 'airflow-secrets-2')
# volume_mount = VolumeMount('test-volume',
#                            mount_path='/root/mount_file',
#                            sub_path=None,
#                            read_only=True)
# port = Port('http', 80)
# configmaps = ['test-configmap-1', 'test-configmap-2']
#
# volume_config= {
#     'persistentVolumeClaim':
#         {
#             'claimName': 'test-volume'
#         }
# }
# volume = Volume(name='test-volume', configs=volume_config)
#
# affinity = {
#     'nodeAffinity': {
#         'preferredDuringSchedulingIgnoredDuringExecution': [
#             {
#                 "weight": 1,
#                 "preference": {
#                     "matchExpressions": {
#                         "key": "disktype",
#                         "operator": "In",
#                         "values": ["ssd"]
#                     }
#                 }
#             }
#         ]
#     },
#     "podAffinity": {
#         "requiredDuringSchedulingIgnoredDuringExecution": [
#             {
#                 "labelSelector": {
#                     "matchExpressions": [
#                         {
#                             "key": "security",
#                             "operator": "In",
#                             "values": ["S1"]
#                         }
#                     ]
#                 },
#                 "topologyKey": "failure-domain.beta.kubernetes.io/zone"
#             }
#         ]
#     },
#     "podAntiAffinity": {
#         "requiredDuringSchedulingIgnoredDuringExecution": [
#             {
#                 "labelSelector": {
#                     "matchExpressions": [
#                         {
#                             "key": "security",
#                             "operator": "In",
#                             "values": ["S2"]
#                         }
#                     ]
#                 },
#                 "topologyKey": "kubernetes.io/hostname"
#             }
#         ]
#     }
# }
#
# tolerations = [
#     {
#         'key': "key",
#         'operator': 'Equal',
#         'value': 'value'
#     }
# ]
#
# k = KubernetesPodOperator(namespace='default',
#                           image="ubuntu:16.04",
#                           cmds=["bash", "-cx"],
#                           arguments=["echo", "10"],
#                           labels={"foo": "bar"},
#                           secrets=[secret_file, secret_env, secret_all_keys],
#                           ports=[port]
# volumes=[volume],
#         volume_mounts=[volume_mount]
# name="test",
#      task_id="task",
#              affinity=affinity,
#                       is_delete_operator_pod=True,
#                                              hostnetwork=False,
#                                                          tolerations=tolerations,
#                                                                      configmaps=configmaps
# )
# Pod Mutation Hook
