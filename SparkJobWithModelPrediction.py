# Importing packages
import numpy as np
import pandas as pd
from sklearn.datasets import load_boston
from sklearn import linear_model

# Machine Learning
boston = load_boston()
dataset = pd.DataFrame(boston.data, columns = boston.feature_names)
dataset['target'] = boston.target
model = linear_model.LinearRegression(normalize = False, fit_intercept = True)
num_observ = len(dataset)
x = dataset['RM'].values.reshape((num_observ, 1))
y = dataset['target'].values
model.fit(x, y)
print(model.predict("Feature value to be predicted")) # X deve sempre ser uma matriz e nunca um vetor