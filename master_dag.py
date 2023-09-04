from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime, timedelta
from pipe_ml_modules import

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 9, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('master_dag',
         default_args=default_args,
         schedule_interval='@daily',
         ) as dag:

    trigger_dag1 = TriggerDagRunOperator(
        task_id='trigger_dag1',
        trigger_dag_id='sub_dag1',
        conf={"message": "Hello World"},
        )

    trigger_dag2 = TriggerDagRunOperator(
        task_id='trigger_dag2',
        trigger_dag_id='sub_dag2',
        conf={"message": "Hello World"},
        )

    trigger_dag3 = TriggerDagRunOperator(
        task_id='trigger_dag3',
        trigger_dag_id='sub_dag3',
        conf={"message": "Hello World"},
        )

    trigger_dag1 >> trigger_dag2 >> trigger_dag3
