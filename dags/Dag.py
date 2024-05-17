# Importing packages
import os
import numpy as np
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.azure.transfers.local_to_adls import LocalFilesystemToADLSOperator
from airflow.providers.microsoft.azure.operators.synapse import AzureSynapseRunSparkBatchOperator

# Defining functions to be used
def _choose_feature_random_value(ti):
    ti.xcom_push(key = my_key, value = 2) # np.random.randint(30, 100)

def _write_feature_random_value_to_local_storage_file(ti):
    feature_random_value = ti.xcom_pull(key = my_key, task_ids = "choose_feature_random_value")
    file = open(os.path.join(local_storage_folder_name + local_remote_storage_file_name), "w")
    file.write(str(feature_random_value))
    file.close() ## Adjust
    
# Defining variables to be used
my_key = str(2) # str(np.random.randint(0, 1000000000))
local_storage_folder_name = "/tmp/"
remote_storage_folder_name = "abfss://dag@storageaccountnamedag.dfs.core.windows.net/synapse/workspaces/workspacenamedag/batchjobs/sparkjobdefinition1/"
local_remote_storage_file_name = "FeatureValue.txt"
local_storage_file_path = os.environ.get(local_storage_folder_name, local_remote_storage_file_name)
remote_storage_file_path = os.environ.get(remote_storage_folder_name, local_remote_storage_file_name)
spark_job = {
                "name": "SparkJobWithModelPrediction",
                "file": remote_storage_folder_name + "SparkJobWithModelPrediction.py",
                "args": [
                    remote_storage_folder_name + "FeatureValue.txt",
                    remote_storage_folder_name, # ???
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
         dag_id = "dag", 
         start_date = datetime(2024, 5, 10), 
         schedule = '*/2 * * * *',
         catchup = False
     ) as dag:

    # Creating tasks/operators
    choose_feature_random_value = PythonOperator(
        task_id = 'choose_feature_random_value',
        python_callable = _choose_feature_random_value
    )

    write_feature_random_value_to_local_storage_file = PythonOperator(
        task_id = 'write_feature_random_value_to_local_storage_file', 
        python_callable = _write_feature_random_value_to_local_storage_file
    ) ## Adjust

    # Defining the flow
    choose_feature_random_value >> write_feature_random_value_to_local_storage_file