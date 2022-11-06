from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.decorators import task
import pendulum


args = {
    'owner': 'Ibrahima',
    'start_date': pendulum.today('UTC'),
    # 'end_date': datetime(2018, 12, 30),
    'depends_on_past': False,
    'email': ['diazp289@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    # Si la tache ne s'execute pas, on laisse au moins 5 minutes
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    'pramod_airflow_dag',
    default_args=args,
    description='A simple DAG',
    # Continue to run DAG once per day
    schedule=timedelta(days=1)
)


# task_1 affiche la date actuelle
task_1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag,
)

task_2 = BashOperator(
    task_id='sleep',
    depends_on_past=False,
    bash_command='sleep 5',
    dag=dag,
)



task_1 >> task_2
