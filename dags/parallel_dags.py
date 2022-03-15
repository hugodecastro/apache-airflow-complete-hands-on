from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.subdag import SubDagOperator

from datetime import datetime
from subdags.subdags_parallel_dags import subdag_parallel_dag

default_args = {
    'start_date': datetime(2020, 1, 1)
}

with DAG('parallel_dag', 
         schedule_interval='@daily', 
         default_args=default_args, catchup=False) as dag:
    
    task_01 = BashOperator(
        task_id='task_01',
        bash_command='sleep 3'
    )

    processing = SubDagOperator(
        task_id='processing_tasks',
        subdag=subdag_parallel_dag(
            parent_dag_id='parallel_dag',
            child_dag_id='processing_tasks',
            default_args=default_args
        )
    )

    task_04 = BashOperator(
        task_id='task_04',
        bash_command='sleep 3'
    )

    task_01 >> processing >> task_04