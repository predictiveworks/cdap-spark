
# Vector Indexer

## Description
This machine learning plugin represents the transformation stage that leverages the Apache Spark ML "Vector Indexer
model" to decide which features are categorical and converts the original values into category indices. This stage 
requires a trained Apache Spark ML "Vector Indexer model".

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Model Configuration
**Model Name**: The unique name of the machine learning model.

**Model Stage***: The stage of the ML model. Supported values are 'experiment', 'staging', 'production'
and 'archived'. Default is 'experiment'.

### Data Configuration
**Input Field**: The name of the field in the input schema that contains the features.

**Output Field**: The name of the field in the output schema that contains the transformed features.
