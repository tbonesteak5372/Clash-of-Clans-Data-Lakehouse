from airflow import DAG, decorators
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from clash_api.api_script import main
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from dotenv import load_dotenv
import os 

load_dotenv()

# arguments for DAG
default_args = {
        'owner': 'clash_of_clans',
        'depends_on_past': False,
        'start_date': datetime(2025,11,1),
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }

# define DAG
with DAG(
    dag_id='clash_of_clans_processing',
    default_args= default_args,
    description= 'Get API data, then send to MinIO bucket to output bucket using PySpark',
    schedule_interval='@monthly',
    catchup=False,
    tags=['spark','minio','etl']
) as dag :


    # api -> minio
    api_to_minio = PythonOperator(
        task_id='get_data_from_api',
        python_callable=main
    )

    spark_submit = SparkSubmitOperator(
        task_id='spark_submit',
        application='/opt/spark/work-dir/jobs/spark_script.py',
        spark_binary='/opt/spark/bin/spark-submit',
        conn_id='spark_conn',
        packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        conf={
            "spark.master": "spark://spark-master:7077",
            "spark.hadoop.fs.s3a.access.key": os.getenv("AWS_ACCESS_KEY_ID"),
            "spark.hadoop.fs.s3a.secret.key": os.getenv("AWS_SECRET_ACCESS_KEY"),
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        }
    )


api_to_minio >> spark_submit


    





