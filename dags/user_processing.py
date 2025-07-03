from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta

@dag(
    schedule='@daily',
    catchup=False,
    start_date=datetime(2025, 1, 3),
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
    def extract_user(char_bio):
        if char_bio is None:
            raise ValueError("No data received from API")
        return {
            "name": char_bio["name"],
            "email": char_bio["email"],
            "city": char_bio["address"]["city"],
            "phone": char_bio["phone"],
            "website": char_bio["website"],
            "company": char_bio["company"]["name"]
        }

    @task
    def process_user(user_data):
        import csv
        with open("/tmp/user_data.csv", "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=user_data.keys())
            writer.writeheader()
            writer.writerow(user_data)

    # Set up dependencies using TaskFlow API
    char_bio = is_api_available()
    user_data = extract_user(char_bio)
    process_user(user_data)

user_processing()