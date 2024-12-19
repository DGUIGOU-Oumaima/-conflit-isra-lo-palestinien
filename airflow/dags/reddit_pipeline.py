from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

dag = DAG('reddit_data_pipeline', default_args=default_args, schedule_interval="@daily")

producer_task = BashOperator(
    task_id='fetch_reddit_data',
    bash_command='python /path/to/kafka/producers/reddit_producer.py',
    dag=dag,
)

consumer_task = BashOperator(
    task_id='process_streaming_data',
    bash_command='python /path/to/kafka/consumers/spark_streaming.py',
    dag=dag,
)

nlp_task = BashOperator(
    task_id='nlp_processing',
    bash_command='python /path/to/spark/jobs/nlp_processing.py',
    dag=dag,
)

sentiment_task = BashOperator(
    task_id='sentiment_analysis',
    bash_command='python /path/to/spark/jobs/sentiment_analysis.py',
    dag=dag,
)

producer_task >> consumer_task >> nlp_task >> sentiment_task
