
# Logistic Regression Classifier

## Description
Logistic regression is a popular method to predict a categorical response. It is a special case of 
Generalized Linear models that predicts the probability of the outcomes. Logistic regression can be 
used to predict a binary outcome by using binomial logistic regression, or it can be used to predict 
a multiclass outcome by using multinomial logistic regression. 

This machine learning plugin represents the building stage for an Apache Spark ML Logistic Regression 
classifier model. This stage expects a dataset with at least two fields to train the model: 

One as an array of numeric values (features), and, another that describes the class or label value as 
numeric value.

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Model Configuration
**Model Name**: The unique name of the machine learning model.

**Model Stage***: The stage of the ML model. Supported values are 'experiment', 'staging', 'production'
and 'archived'. Default is 'experiment'.

### Data Configuration

### Parameter Configuration
