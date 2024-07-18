from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer

def download_and_process_nltk():
    # Download the VADER lexicon for sentiment analysis
    nltk.download('vader_lexicon')
    
    # Initialize the SentimentIntensityAnalyzer
    sia = SentimentIntensityAnalyzer()
    
    # Example text for sentiment analysis
    text = "Airflow is an amazing tool for managing workflows!"
    
    # Perform sentiment analysis
    sentiment_score = sia.polarity_scores(text)
    print("Sentiment analysis result:")
    print(sentiment_score)

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'test_nltk_dag',
    default_args=default_args,
    description='A simple DAG to test nltk library by downloading and processing a dataset',
    schedule_interval='@daily',  # Set to None or a specific schedule interval
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Define the task
    process_nltk_task = PythonOperator(
        task_id='download_and_process_nltk',
        python_callable=download_and_process_nltk,
    )

    # Set task dependencies (if you had more tasks, you would set dependencies here)
    process_nltk_task
