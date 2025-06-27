# Airflow Project 1

## Initial setup connecting to Airflow/Docker

1. After adding the docker-compose.yaml file, set up the Python virtual environment. Use the following command:
uv venv --python 3.12.6 - (or whatever Python version you have installed). If you haven't got UV installed you will need to install that.

2. Activate the virtual environment by typing in the command in the terminal, source .venv/bin/activate

3. Install Airflow using the command:
 uv pip install apache-airflow==3.0.0

4. Verify no other instances on your Docker and make sure nothing else is running, and if not then stop them.

5. Then in the terminal run the following:
 docker compose up 

6. Wait for the previous step. Click on the port to the right of airflow-apiserver-1 and sign in to airflow using the username and passwords, both with airflow (this is standard I think??). Then you will be signed in to the Airflow UI.

To create a connection to the postgresql database on Airflow:

1. In the Airflow UI, click on admin and then connections.
2. On the page you are directed to click 'Add connection'.
3. Then enter the following details on that page:
    - connection id - postgres
    - connection type - postgres (in the dropdown)
    - description -  This is the airflow database
    - host - postgres
    - login - airflow
    - password - airflow
    - port - 5432
4. Click save and you should be connected to the postgresql database.
   