from airflow.decorators import task
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator


dag = DAG(
    dag_id="a_mac_branch_modelo_1b",
    schedule="@daily",
    start_date=days_ago(1),
)

inicio = EmptyOperator(task_id="inicio", dag=dag)


@task.branch(task_id="branching")
def do_branching():
    return "tarea_1"


branching = do_branching()

tarea_1 = EmptyOperator(task_id="tarea_1", dag=dag)
tarea_2 = EmptyOperator(task_id="tarea_2", dag=dag)

tarea_alterna = EmptyOperator(task_id="tarea_alterna", dag=dag)

end_of_flow = EmptyOperator(task_id="end_of_flow", dag=dag)

inicio >> branching >> [tarea_1,tarea_alterna]
tarea_1 >> tarea_2 >> end_of_flow
tarea_alterna >> end_of_flow