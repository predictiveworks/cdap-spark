
# Gradient Boosted-Tree Regressor

## Description

This machine learning plugin represents the building stage for an Apache Spark ML "Gradient-Boosted Tree 
regressor model". This stage expects a dataset with at least two fields to train the model: One as an array 
of numeric values, and, another that describes the class or label value as numeric value.

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Model Configuration
**Model Name**: The unique name of the machine learning model.

**Model Stage***: The stage of the ML model. Supported values are 'experiment', 'staging', 'production'
and 'archived'. Default is 'experiment'.

### Data Configuration

### Parameter Configuration
