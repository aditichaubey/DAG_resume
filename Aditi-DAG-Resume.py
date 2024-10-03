from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from google.cloud import storage

# Define the bucket and file paths
BUCKET_NAME = 'us-east1-dev-composer-bc71c2fb-bucket'
FILE_PATHS = {
    'Profile': 'sql/Resume/Profile.csv',
    'Experience': 'sql/Resume/Experience.csv',
    'Skills': 'sql/Resume/Skills.csv',
    'Certifications': 'sql/Resume/Certifications.csv',
    'Achievements': 'sql/Resume/Achievements.csv',
    'Education': 'sql/Resume/Education.csv'
}

def read_csv_from_gcs(file_name):
    """Read a CSV file from Google Cloud Storage and print its contents."""
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(file_name)
    data = blob.download_as_text()
    
    # Use pandas to read the CSV data
    df = pd.read_csv(pd.compat.StringIO(data))
    print(f"Data from {file_name}:\n", df)

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

# Create the DAG
with DAG('read_csv_dag',
         default_args=default_args,
         schedule_interval='@daily',  # Set your schedule
         catchup=False) as dag:

    # Create tasks for each CSV file
    profile_task = PythonOperator(
        task_id='read_profile',
        python_callable=read_csv_from_gcs,
        op_kwargs={'file_name': FILE_PATHS['Profile']},
    )

    experience_task = PythonOperator(
        task_id='read_experience',
        python_callable=read_csv_from_gcs,
        op_kwargs={'file_name': FILE_PATHS['Experience']},
    )

    skills_task = PythonOperator(
        task_id='read_skills',
        python_callable=read_csv_from_gcs,
        op_kwargs={'file_name': FILE_PATHS['Skills']},
    )

    certifications_task = PythonOperator(
        task_id='read_certifications',
        python_callable=read_csv_from_gcs,
        op_kwargs={'file_name': FILE_PATHS['Certifications']},
    )

    achievements_task = PythonOperator(
        task_id='read_achievements',
        python_callable=read_csv_from_gcs,
        op_kwargs={'file_name': FILE_PATHS['Achievements']},
    )

    education_task = PythonOperator(
        task_id='read_education',
        python_callable=read_csv_from_gcs,
        op_kwargs={'file_name': FILE_PATHS['Education']},
    )

    # Set task dependencies to run in sequence
    profile_task >> experience_task >> skills_task >> certifications_task >> achievements_task >> education_task