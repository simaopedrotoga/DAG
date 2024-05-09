# Importing packages
import numpy as np
import pandas as pd
from sklearn.datasets import fetch_california_housing
from sklearn import linear_model

# Import feature value
remote_storage_folder_name = "abfss://dag@storageaccountnamedag.dfs.core.windows.net/synapse/workspaces/workspacenamedag/batchjobs/sparkjobdefinition1/"
local_remote_storage_file_name = "FeatureValue.txt"
remote_storage_file_name = "PredictedValue.txt"
remote_storage_file_path = remote_storage_folder_name + local_remote_storage_file_name
feature_value = int(pd.read_csv(remote_storage_file_path).columns[0])

# Machine Learning
housing = fetch_california_housing()
dataset = pd.DataFrame(housing.data, columns = housing.feature_names)
dataset['target'] = housing.target
model = linear_model.LinearRegression(fit_intercept = True)
num_observ = len(dataset)
x = dataset['MedInc'].values.reshape((num_observ, 1))
y = dataset['target'].values
model.fit(x, y)
predicted_value = float(model.predict(np.asarray([[feature_value]]))[0])
pd.DataFrame({"Predicted Value": [predicted_value]}).to_csv(remote_storage_folder_name + remote_storage_file_name, index = False)