import airflow
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime, timezone

default_args = {
    "owner": "harry.cheng",
    "retries": 10,
    "retry_delay": timedelta(minutes=10),
}

@task(task_id='extract_data')
def extract_data(db: str):
    result = f'the data is from {db}'
    return result

@task(task_id='transform_data')
def transform_data(data):
    data_transform = data + '!!!!!'
    return data_transform

@task(task_id='load_data')
def load_data(db, data_transform):
    # store data somewhere else
    print(f'{data_transform} from {db} done')

@task(task_id='concat')
def concat_all(*args):
    # concat every result together once they are all finished
    print('concat all result')

with DAG(
    dag_id="python_operator_test",
    description="it is an example using task decorator and python operator",
    schedule_interval="00 10 * * 1-5",
    start_date=datetime(2023, 12, 21, 10, 0, 0, tzinfo=timezone(timedelta(hours=+8))),
    catchup=False,
    default_args=default_args,
) as dag:
    
    databases = ['db1', 'db2', 'db3', 'db4', 'db5']
    load_data_tasks = []
    for db in databases:
        data = extract_data(db=db)
        data_transform = transform_data(data=data)
        load_data_result = load_data(db=db, data_transform=data_transform)
        load_data_tasks.append(load_data_result)
    concat_all(*load_data_tasks)