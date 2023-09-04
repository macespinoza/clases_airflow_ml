from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 9, 1),
}

def initiate_variable():
    return 10

def add_value(value, **kwargs):
    return value + 10

def multiply_value(value, **kwargs):
    return value * 2

def branch(value, **kwargs):
    if value > 20:
        return 'add_or_multiply_group.addition_in_branch'
    else:
        return 'add_or_multiply_group.multiplication_in_branch'

def division_callable(**kwargs):
    ti = kwargs['ti']
    addition_result = ti.xcom_pull(task_ids='add_or_multiply_group.addition_in_branch')
    multiplication_result = ti.xcom_pull(task_ids='add_or_multiply_group.multiplication_in_branch')
    if addition_result:
        return addition_result / 10
    elif multiplication_result:
        return multiplication_result / 10
    else:
        raise ValueError('No result found for addition or multiplication tasks')

with DAG('task_group_and_branching_dag',
         default_args=default_args,
         schedule_interval=None) as dag:

    initiate_task = PythonOperator(
        task_id='initiate_variable',
        python_callable=initiate_variable,
        do_xcom_push=True,
    )

    with TaskGroup('add_or_multiply_group') as add_or_multiply_group:

        addition_task = PythonOperator(
            task_id='addition',
            python_callable=add_value,
            op_args=[initiate_task.output],
            do_xcom_push=True,
        )

        branching_task = BranchPythonOperator(
            task_id='branching_task',
            python_callable=branch,
            op_args=[addition_task.output],
        )

        addition_in_branch = PythonOperator(
            task_id='addition_in_branch',
            python_callable=add_value,
            op_args=[addition_task.output],
            do_xcom_push=True,
        )

        multiplication_in_branch = PythonOperator(
            task_id='multiplication_in_branch',
            python_callable=multiply_value,
            op_args=[addition_task.output],
            do_xcom_push=True,
        )

        join_task = DummyOperator(
            task_id='join',
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        )

        addition_task >> branching_task >> [addition_in_branch, multiplication_in_branch] >> join_task

    division_task = PythonOperator(
        task_id='division',
        python_callable=division_callable,
        provide_context=True,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )


    initiate_task >> add_or_multiply_group >> division_task
