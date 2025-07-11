from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime
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
            name VARCHAR(255),
            height VARCHAR(255),
            weight VARCHAR(255),
            base_experience VARCHAR(255),
            type VARCHAR(255)
        );
        """
    )

    @task
    def is_api_available():
        import requests
        response = requests.get("https://pokeapi.co/api/v2/pokemon/pikachu")
        print(response.status_code)
        print(response.text)
        if response.status_code == 200:
            return response.json()
        else:
            return None

    @task 
    def extract_user(poke_data):
        if poke_data is None:
            raise ValueError("No data received from API")
        return {
            "id": poke_data["id"],
            "name": poke_data["name"],
            "height": poke_data["height"],
            "weight": poke_data["weight"],
            "base_experience": poke_data["base_experience"],
            "type": poke_data["types"][0]["type"]["name"]
        }

    @task
    def process_user(extracted_poke):
        import csv
        with open("/tmp/poke_data.csv", "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=extracted_poke.keys())
            writer.writeheader()
            writer.writerow(extracted_poke)

    @task
    def store_user():
        hook = PostgresHook(postgres_conn_id='postgres')
        hook.copy_expert(
            sql="COPY users (id, name, height, weight, base_experience, type) FROM STDIN WITH CSV HEADER",
            filename="/tmp/poke_data.csv"
        )

    # Execute tasks and define dependencies
    poke_data = is_api_available()
    extracted = extract_user(poke_data)
    processed = process_user(extracted)
    stored = store_user()

    # Define execution order
    create_table >> poke_data
    processed >> stored
    
user_processing()