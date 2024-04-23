# Importing packages
import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.azure.transfers.local_to_adls import LocalFilesystemToADLSOperator
from airflow.providers.microsoft.azure.operators.synapse import AzureSynapseRunSparkBatchOperator

# Defining functions to be used
def _choose_random_customer_id(ti):
    ti.xcom_push(key = "my_key", value = "42") # Adjust

def _write_id_to_local_storage_file(ti):
    _id = ti.xcom_pull(key = "my_key", task_ids = "choose_random_customer_id") 
    file = open(os.path.join(local_storage_folder_name + local_storage_file_name, 'w')
    file.write(_id) 
    file.close() ## Adjust
    
# Defining variables to be used
local_storage_folder_name = "/tmp/"
remote_storage_folder_name = "REMOTE_LOCAL_PATH"
local_storage_file_name = "lcustomer.txt"
remote_storage_file_name = "acustomer.txt"
local_storage_file_path = os.environ.get(local_storage_folder_name, local_storage_file_name)
remote_storage_file_path = os.environ.get(remote_storage_folder_name, remote_storage_file_name)
spark_job_payload = {
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
    choose_random_customer_id = PythonOperator(
        task_id = 'choose_random_customer_id',
        python_callable = _choose_random_customer_id
    ) ## Adjust
    
    write_id_to_local_storage_file = PythonOperator(
        task_id = 'write_id_to_local_storage_file', 
        python_callable = _write_id_to_local_storage_file
    ) ## Adjust

    transfer_local_storage_file_to_azure_blob_storage = LocalFilesystemToADLSOperator(
        task_id = "transfer_local_storage_file_to_azure_blob_storage",
        local_path = local_storage_file_path,
        remote_path = remote_storage_file_path
    ) ## Adjust

    run_spark_job = AzureSynapseRunSparkBatchOperator(
                        task_id = "run_spark_job", 
                        spark_pool = "provsparkpool", 
                        payload = SPARK_JOB_PAYLOAD  ## Adjust
    ) ## Adjust

    # Defining the flow
    create_table >> is_api_available >> extract_user >> process_user >> store_user ## Adjust