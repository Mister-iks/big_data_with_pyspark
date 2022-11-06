from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import pendulum
from chapitre02.DataFrame import chapitre2
from chapitre03.App import chapitre3
from chapitre05.binaryClass import chapitre5
from chapitre07.cluster import chapitre7

dag = DAG(
    dag_id="ibrahima_dag",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    render_template_as_native_obj=True,
)

chapitre_2_task = PythonOperator(
    task_id="chapitre_2_task",
    python_callable=chapitre2,
    dag=dag
)
chapitre_3_task = PythonOperator(
    task_id="chapitre_3_task",
    python_callable=chapitre3,
    dag=dag
)
chapitre_5_task = PythonOperator(
    task_id="chapitre_5_task",
    python_callable=chapitre5,
    dag=dag
)
chapitre_7_task = PythonOperator(
    task_id="chapitre_7_task",
    python_callable=chapitre7,
    dag=dag
)
