
# Isotonic Regressor

## Description

This machine learning plugin represents the building stage for an Apache Spark ML "Isotonic Regression (regressor) model".
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
**Isotonic Indicator**: This indicator determines whether the output sequence should be 'isotonic' (increasing) 
or 'antitonic' (decreasing). Default is 'isotonic'.

**Feature Index**: The nonnegative index of the feature. Default is 0.
