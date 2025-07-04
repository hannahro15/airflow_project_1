from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook


@dag(
    schedule='@daily',
    catchup=False,
    start_date=datetime(2025, 1, 4),
    tags=['example', 'user']
)
def user_processing():
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS users (
            id INT PRIMARY KEY,
            firstname VARCHAR(255),
            lastname VARCHAR(255),
            email VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    @task
    def is_api_available():
        import requests
        response = requests.get("https://jsonplaceholder.typicode.com/users/1")
        print(response.status_code)
        print(response.text)
        if response.status_code == 200:
            return response.json()
        else:
            return None

    @task
    def extract_user(user_data):
        if user_data is None:
            raise ValueError("No data received from API")
        return {
            "name": user_data["name"],
            "email": user_data["email"],
            "city": user_data["address"]["city"],
            "phone": user_data["phone"],
            "website": user_data["website"],
            "company": user_data["company"]["name"]
        }

    @task
    def process_user(extracted_user):
        import csv
        with open("/tmp/user_data.csv", "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=extracted_user.keys())
            writer.writeheader()
            writer.writerow(extracted_user)

    @task
    def store_user(dummy=None):
        hook = PostgresHook(postgres_conn_id='postgres')
        hook.copy_expert(
            sql="COPY users FROM STDIN WITH CSV HEADER",
            filename="/tmp/user_data.csv"
        )

    user_data = is_api_available()
    extracted_user = extract_user(user_data)
    csv_written = process_user(extracted_user)
    store_user(csv_written)
    create_table >> user_data


user_processing()