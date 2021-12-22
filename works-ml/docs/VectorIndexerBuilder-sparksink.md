
# Vector Indexer Builder

## Description
This machine learning plugin represents the building stage for an Apache Spark ML "Vector Indexer model".

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Model Configuration
**Model Name**: The unique name of the machine learning model.

**Model Stage***: The stage of the ML model. Supported values are 'experiment', 'staging', 'production'
and 'archived'. Default is 'experiment'.

### Data Configuration
**Input Field**: The name of the field in the input schema that contains the features to build the model from.

### Parameter Configuration
**Maximum Categories**: The threshold for the number of values a categorical feature can take. If a feature is 
found to have more category values than this threshold, then it is declared continuous. Must be greater than or 
equal to 2. Default is 20.