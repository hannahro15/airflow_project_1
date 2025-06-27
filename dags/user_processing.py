from airflow.sdk import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

@dag
def user_processing():
    
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        CONN_ID="postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS user (
        id INT PRIMARY KEY,
        firstname VARCHAR(255),
        lastname VARCHAR(255),
        email VARCHAR(255),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """

    )

user_processing()