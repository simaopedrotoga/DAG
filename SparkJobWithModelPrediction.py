# Importing packages
import numpy as np
import pandas as pd
from sklearn.datasets import fetch_california_housing
from sklearn import linear_model

# Import feature value
feature_value = 0

# Machine Learning
housing = fetch_california_housing()
dataset = pd.DataFrame(housing.data, columns = housing.feature_names)
dataset['target'] = housing.target
model = linear_model.LinearRegression(fit_intercept = True)
num_observ = len(dataset)
x = dataset['MedInc'].values.reshape((num_observ, 1))
y = dataset['target'].values
model.fit(x, y)
print(model.predict(np.asarray([[feature_value]])))