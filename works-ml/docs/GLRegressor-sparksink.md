
# Generalized Linear Regressor

## Description

This machine learning plugin represents the building stage for an Apache Spark ML "Generalized Linear regression
model". This stage expects a dataset with at least two fields to train the model: One as an array of numeric values, 
and, another that describes the class or label value as numeric value.

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Model Configuration
**Model Name**: The unique name of the machine learning model.

**Model Stage***: The stage of the ML model. Supported values are 'experiment', 'staging', 'production'
and 'archived'. Default is 'experiment'.

### Data Configuration
**Features Field**: The name of the field in the input schema that contains the feature vector.

**Label Field**: The name of the field in the input schema that contains the label.

**Data Split**: The split of the dataset into train & test data, e.g. 80:20. Default is 70:30.

### Parameter Configuration
**Maximum Iterations**: The maximum number of iterations to train the Linear Regression model. Default is 25.

**Regularization Parameter**: The nonnegative regularization parameter. Default is 0.0.

**Conversion Tolerance**: The positive convergence tolerance of iterations. Smaller values will lead to higher 
accuracy with the cost of more iterations. Default is 1e-6.

**Distribution Family**: The name of the family which is a description of the error distribution used in this 
model. Supported values are: 'gaussian', 'binomial', 'poisson' and 'gamma'. The family values are correlated
with the name of the link function. Default is 'gaussian'.

**Link Function**: The name of the link function which provides the relationship between the linear predictor, 
and the mean of the distribution function. Supported values are: 'identity', 'log', 'inverse', 'logit', 'probit', 
'cloglog' and 'sqrt'. Default is 'identity' (gaussian).

