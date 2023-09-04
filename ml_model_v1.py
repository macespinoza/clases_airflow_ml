import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
from datetime import datetime, timedelta
import joblib
import gcsfs

def load_data():
    # Crear un objeto file system
    fs = gcsfs.GCSFileSystem(project='automatic-tract-396023')
    
    # Leer los datos desde GCS
    with fs.open('gs://automatic-tract-396023-datasetml/titanic_train_csv.csv') as f:
        data = pd.read_csv(f,sep=";")
    
    return data
    
def load_data(table_id,uri):
    client = bigquery.Client();

    table = client.get_table(table_id)

    job_config = bigquery.LoadJobConfig(
        schema=table.schema,
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.CSV,
    )
    job_config.field_delimiter = ";"
    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )

def train_model(data):
    # Preparar los datos
    X = data.drop('target', axis=1)
    y = data['target']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    # Entrenar el modelo
    clf = RandomForestClassifier(random_state=42)
    clf.fit(X_train, y_train)
    
    # Guardar el modelo entrenado
    joblib.dump(clf, 'path/to/save/your/model.pkl')
    
    return X_test, y_test

def validate_model(X_test, y_test):
    # Cargar el modelo entrenado
    clf = joblib.load('path/to/save/your/model.pkl')
    
    # Realizar predicciones
    y_pred = clf.predict(X_test)
    
    # Calcular la precisión
    accuracy = accuracy_score(y_test, y_pred)
    
    return accuracy

def deploy_model():
    # Código para desplegar tu modelo
    # Esto dependerá de cómo planeas desplegar tu modelo
    ...
    
    return 'Model deployed successfully'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 9, 1),
    'retries': 1,
}

dag = DAG(
    'machine_learning_pipeline',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval='@daily',
)

load_data_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

train_model_task = PythonOperator(
    task_id='train_model',
    python_callable=train_model,
    provide_context=True,
    dag=dag,
)

validate_model_task = PythonOperator(
    task_id='validate_model',
    python_callable=validate_model,
    provide_context=True,
    dag=dag,
)

deploy_model_task = PythonOperator(
    task_id='deploy_model',
    python_callable=deploy_model,
    dag=dag,
)

load_data_task >> train_model_task >> validate_model_task >> deploy_model_task

