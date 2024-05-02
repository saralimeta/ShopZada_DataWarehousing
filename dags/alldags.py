from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor

dag = DAG(
    'DagFiles',
    default_args = {
        'depends_on_past': True,
        'retries': 1,
        'retry-delay': timedelta(seconds=30),
    },
    start_date = datetime(2023, 12, 10),
    schedule = timedelta(minutes =90),
    catchup = False,
)

trigger_dag1 = TriggerDagRunOperator(
    task_id='trigger_dag1',
    trigger_dag_id='MerchantDag',
    dag=dag,
)

# Trigger dag2
trigger_dag2 = TriggerDagRunOperator(
    task_id='trigger_dag2',
    trigger_dag_id='StaffDag',
    dag=dag,
)

trigger_dag1 >> trigger_dag2
