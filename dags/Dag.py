# Importing packages
import os
import numpy as np
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.azure.transfers.local_to_adls import LocalFilesystemToADLSOperator
from airflow.providers.microsoft.azure.operators.synapse import AzureSynapseRunSparkBatchOperator

from airflow.models import XCom
from airflow.utils.db import provide_session

# Defining functions to be used
def _choose_feature_random_value(ti):
    ti.xcom_push(key = my_key, value = np.random.randint(30, 100))

def _write_feature_random_value_to_local_storage_file(ti):
    feature_random_value = ti.xcom_pull(key = my_key, task_ids = "choose_feature_random_value")

    print("::group::Non important details")
    print(str(feature_random_value))
    print("::endgroup::")

    file = open(os.path.join(local_storage_folder_name + local_remote_storage_file_name), "w")
    file.write(str(feature_random_value))
    file.close() ## Adjust
    arquivo = open(os.path.join(local_storage_folder_name + local_remote_storage_file_name), "r")
    conteudo = arquivo.read()
    arquivo.close()

    print("::group::Non important details")
    print(conteudo)
    print("::endgroup::")
    
# Defining variables to be used
my_key = str(np.random.randint(0, 1000000000))
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
         schedule = '*/1 * * * *',
         catchup = False
     ) as dag:


    @provide_session
    def clean_xcom(session = None, **context):
        dag = context["dag"]
        dag_id = dag._dag_id 
        session.query(XCom).filter(XCom.dag_id == dag_id).delete()

    delete_xcom = PythonOperator(
        task_id = "delete_xcom",
        python_callable = clean_xcom, 
        dag = dag
    )
    

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
    delete_xcom >> choose_feature_random_value >> write_feature_random_value_to_local_storage_file