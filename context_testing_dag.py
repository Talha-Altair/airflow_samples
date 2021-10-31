from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def chumma_sample(**context):

    name = context.get('name',"NO NAME")

    print(f"hello {name}")

    context['ti'].xcom_push(key="age",value = "10")

    return "success " + name

def chumma_sample2(**context):

    name = context.get('name',"NO NAME")

    print(f"hello {name}")

    age = context.get('ti').xcom_pull(key = "age")

    print("age:",age)

    return "success " + name + age

with DAG(
        dag_id="context_testing_dag",
        schedule_interval="* * * * *",
        default_args={
            "owner": "airflow",
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
            "start_date": datetime(2021, 1, 1),
        },
        catchup=False) as f:

    first_task = PythonOperator(
        task_id="chumma_task",
        python_callable=chumma_sample,
        op_kwargs={"name":"altair"},
        provide_context=True,
    )

    second_task = PythonOperator(
        task_id="chumma_task_2",
        python_callable=chumma_sample2,
        op_kwargs={"name":"talha"},
        provide_context=True,
    )

first_task >> second_task