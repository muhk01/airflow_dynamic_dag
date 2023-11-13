from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

# Task Definition
path_script = "/home/path-yours/dag_script"

def generate_ingestion_dag(table_ingest_name, table_serving_name=None):
    dag_id = f'ingestion_dag_{table_ingest_name}'
    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule_interval='*/5 * * * *'
    )

    start_task = DummyOperator(task_id='start', dag=dag)

    ingest_table = BashOperator(
        task_id=f'ingestion_table_{table_ingest_name}',
        bash_command="{}/{} ".format(path_script, table_ingest_name),
        dag=dag,
    )

    end_ingestion = DummyOperator(task_id='end_ingestion', dag=dag)

    # Dynamic Flow
    if table_serving_name:
        serving_data = BashOperator(
            task_id=f'serving_table_{table_ingest_name}',
            bash_command="{}/{} ".format(path_script, table_serving_name),
            dag=dag,
        )
        start_task >> ingest_table >> serving_data >> end_ingestion
    else:
        start_task >> ingest_table >> end_ingestion
    return dag


# Define default_args
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# Generate dynamic DAGs for DATA INGESTION
# args -> step1 >> step2
dag_ingestion_table1 = generate_ingestion_dag('table_data1.sh')
dag_ingestion_table2 = generate_ingestion_dag('table_data2.sh')
dag_ingestion_table3 = generate_ingestion_dag('table_data3.sh', 'serving_data1.sh')
dag_ingestion_table4 = generate_ingestion_dag('table_data4.sh', 'serving_data2.sh')

