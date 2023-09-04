from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

dag = DAG(
    dag_id="a_mac_branch_modelo_2",
    #schedule="@daily",
    start_date=days_ago(1),
    schedule_interval='0 23 * * *',
)

inicio = EmptyOperator(task_id="inicio", dag=dag)

def python_branch(**context):
    var=0
    if var == 0:
        return 'tarea_1'
    else:
        return 'tarea_alterna'
        

branching = BranchPythonOperator(
    task_id='branching',
    python_callable=python_branch,
    provide_context=True,
    dag=dag,
)

tarea_1 = EmptyOperator(task_id="tarea_1", dag=dag)
tarea_2 = EmptyOperator(task_id="tarea_2", dag=dag)

tarea_alterna = EmptyOperator(task_id="tarea_alterna", dag=dag)

end_of_flow = EmptyOperator(task_id="end_of_flow", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS, dag=dag)

inicio >> branching
branching >> tarea_1 >> tarea_2 >> end_of_flow
branching >> tarea_alterna >> end_of_flow