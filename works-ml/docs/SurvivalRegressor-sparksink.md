
# Survival Regressor

## Description

This machine learning plugin represents the building stage for an Apache Spark ML "Survival (AFT) regression model". 
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

**Censor Field**: The name of the field in the input schema that contains the censor value. The censor value
can be 0 or 1. If the value is 1, it means the event has occurred (uncensored); otherwise censored.

**Data Split**: The split of the dataset into train & test data, e.g. 80:20. Default is 70:30.

### Parameter Configuration
**Maximum Iterations**: The maximum number of iterations to train the AFT Survival Regression model. Default is 100.

**Conversion Tolerance**: The positive convergence tolerance of iterations. Smaller values will lead to higher 
accuracy with the cost of more iterations. Default is 1e-6.
