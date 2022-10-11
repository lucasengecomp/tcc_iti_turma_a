from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta
from airflow.contrib.sensors.file_sensor import FileSensor

###############################################
# Parameters
###############################################
spark_master = "spark://spark:7077"

###############################################
# DAG Definition
###############################################
now = datetime.now()

pipeline = "simplePipelineV2"
modelInfoModel = "teste,frequency,recency,T,id"
output = now.strftime("%H%M%S")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(now.year, now.month, now.day),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
        dag_id="Processamento_Modelo", 
        description="DAG Processamento Modelo",
        default_args=default_args, 
        schedule_interval= "0 */5 * * *"
    )

start = DummyOperator(task_id="start", dag=dag)

sensor_task = FileSensor( task_id= "file_sensor_task", poke_interval= 10, fs_conn_id="my_file_system", filepath= "/usr/local/spark/resources/data/dataset", dag=dag )

spark_job = SparkSubmitOperator(
    task_id="spark_job",
    application="/usr/local/spark/app/runPipeline.py",
    name="runPipeline",
    conn_id="spark_default",
    verbose=1,
    conf={"spark.master":spark_master},
    application_args=[pipeline, modelInfoModel, output],
    dag=dag)

end = DummyOperator(task_id="end", dag=dag)

start >> sensor_task >> spark_job >> end