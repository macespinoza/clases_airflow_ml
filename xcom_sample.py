from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 9, 1),
}

def add_numbers(**kwargs):
    result = 5 + 3
    kwargs['ti'].xcom_push(key='addition_result', value=result)

def multiply_numbers(**kwargs):
    ti = kwargs['ti']
    addition_result = ti.xcom_pull(key='addition_result', task_ids='addition_task')
    result = addition_result * 4
    ti.xcom_push(key='multiplication_result', value=result)

def subtract_numbers(**kwargs):
    ti = kwargs['ti']
    multiplication_result = ti.xcom_pull(key='multiplication_result', task_ids='multiplication_task')
    result = multiplication_result - 10
    ti.xcom_push(key='subtraction_result', value=result)

def divide_numbers(**kwargs):
    ti = kwargs['ti']
    subtraction_result = ti.xcom_pull(key='subtraction_result', task_ids='subtraction_task')
    result = subtraction_result / 2
    print(f"The final result is: {result}")

with DAG('math_operations_dag',
         default_args=default_args,
         schedule_interval=None) as dag:

    addition_task = PythonOperator(
        task_id='addition_task',
        python_callable=add_numbers,
        provide_context=True
    )

    multiplication_task = PythonOperator(
        task_id='multiplication_task',
        python_callable=multiply_numbers,
        provide_context=True
    )

    subtraction_task = PythonOperator(
        task_id='subtraction_task',
        python_callable=subtract_numbers,
        provide_context=True
    )

    division_task = PythonOperator(
        task_id='division_task',
        python_callable=divide_numbers,
        provide_context=True
    )

    # Definimos el orden de ejecución
    addition_task >> multiplication_task >> subtraction_task >> division_task
