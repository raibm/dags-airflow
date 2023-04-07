from airflow import DAG
from datetime import datetime
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator




default_args = {
    "owner": "rai",
    "start_date": datetime(2023, 1, 25),
}


dag = DAG(
    "dag_rai_teste_003",
    default_args=default_args,
    schedule_interval="15 10 * * 1-5",
    max_active_runs=1,
)


first_task = DummyOperator(
    task_id="first_task",
    dag=dag
)


def test_function():
    print('Hello World!')


task_python_operator = PythonOperator(
    task_id='task_python_operator',
    python_callable=test_function,
    dag=dag
)


second_task = DummyOperator(
    task_id="second_task",
    dag=dag
)


last_task = DummyOperator(
    task_id="last_task",
    dag=dag
)


first_task >> task_python_operator >> second_task >> last_task