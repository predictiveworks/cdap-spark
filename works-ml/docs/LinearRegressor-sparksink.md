
# Linear Regressor

## Description

This machine learning plugin represents the building stage for an Apache Spark ML "Linear Regression (regressor) model". 
This stage expects a dataset with at least two fields to train the model: One as an array of numeric values, and, 
another that describes the class or label value as numeric value.

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
**Maximum Iterations**: The maximum number of iterations to train the Linear Regression model. Default is 100.

**ElasticNet Mixing**: The ElasticNet mixing parameter. For value = 0.0, the penalty is an L2 penalty. For value 
= 1.0, it is an L1 penalty. For 0.0 < value < 1.0, the penalty is a combination of L1 and L2. Default is 0.0.

**Regularization Parameter**: The nonnegative regularization parameter. Default is 0.0.

**Conversion Tolerance**: The positive convergence tolerance of iterations. Smaller values will lead to higher 
accuracy with the cost of more iterations. Default is 1e-6.

**Solver Algorithm**: The solver algorithm for optimization. Supported options are 'auto', 'normal' and 'l-bfgs'. 
Default is 'auto'.
