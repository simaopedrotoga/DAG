# Importing packages
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.azure.operators.synapse import AzureSynapseRunSparkBatchOperator

# Defining functions to be used
def _process_user(ti):
    user = ti.xcom_pull(task_ids = "extract_user")
    user = user['results'][0]
    processed_user = json_normalize({
        'firstname': user['name']['first'],
        'lastname': user['name']['last'],
        'country': user['location']['country'],
        'username': user['login']['username'],
        'password': user['login']['password'],
        'email': user['email'] })
    processed_user.to_csv('/tmp/processed_user.csv', index = None, header = False) ## Adjust

# Defining variables to be used
SPARK_JOB_PAYLOAD = {
    "name": "SparkJob",
    "file": "abfss://spark@providersstorageaccgen2.dfs.core.windows.net/wordcount.py",
    "args": [
        "abfss://spark@providersstorageaccgen2.dfs.core.windows.net/shakespeare.txt",
        "abfss://spark@providersstorageaccgen2.dfs.core.windows.net/results/",
    ],
    "jars": [],
    "pyFiles": [],
    "files": [],
    "conf": {
        "spark.dynamicAllocation.enabled": "false",
        "spark.dynamicAllocation.minExecutors": "1",
        "spark.dynamicAllocation.maxExecutors": "2",
    },
    "numExecutors": 2,
    "executorCores": 4,
    "executorMemory": "28g",
    "driverCores": 4,
    "driverMemory": "28g",
} ## Adjust

# Creating scheduled DAGs
with DAG(
         dag_id = "user_processing", 
         start_date = datetime(2022, 1, 1), 
         schedule = "@daily", 
         catchup = False
     ) as dag: ## Adjust

    # Creating tasks/operators
    extract_user = SimpleHttpOperator(task_id = "extract_user",
                                      http_conn_id = "user_api",
                                      endpoint = "api/",
                                      method = "GET",
                                      response_filter = lambda response: json.loads(response.text),
                                      log_response = True) ## Adjust
    
    process_user = PythonOperator(task_id = 'process_user', python_callable = _process_user) ## Adjust

    run_spark_job = AzureSynapseRunSparkBatchOperator(
                        task_id = "run_spark_job", 
                        spark_pool = "provsparkpool", 
                        payload = SPARK_JOB_PAYLOAD  ## Adjust
                    )

    # Defining the flow
    create_table >> is_api_available >> extract_user >> process_user >> store_user ## Adjust