
# TF IDF Builder

## Description
This machine learning plugin represents the building stage for an Apache Spark ML "TF-IDF feature model".

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Model Configuration
**Model Name**: The unique name of the machine learning model.

**Model Stage***: The stage of the ML model. Supported values are 'experiment', 'staging', 'production'
and 'archived'. Default is 'experiment'.

### Data Configuration
**Input Field**: The name of the field in the input schema that contains the features to build the model from.

### Parameter Configuration
**Number of Features**: The nonnegative number of features to transform a sequence of terms into.

**Minimum Document Frequency**: The minimum number of documents in which a term should appear. Default is 0.
