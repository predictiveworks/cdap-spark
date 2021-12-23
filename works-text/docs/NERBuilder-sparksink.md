
# NER Builder

## Description
This machine learning plugin represents the building stage for an Apache Spark NLP "NER (CRF) model".

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Model Configuration
**Model Name**: The unique name of the machine learning model.

**Model Stage***: The stage of the ML model. Supported values are 'experiment', 'staging', 'production'
and 'archived'. Default is 'experiment'.

### Data Configuration
**Token Field**: The name of the field in the input schema that contains the labeled tokens.

### Parameter Configuration
**Embedding Name**: The unique name of the trained Word2Vec embedding model.

**Embedding Stage**: The stage of the Word2Vec embedding model. Supported values are 'experiment', 'staging',
'production' and 'archived'. Default is 'experiment'.

**Minimum Epochs**: Minimum number of epochs to train. Default is 10.

**Maximum Epochs**: Maximum number of epochs to train. Default is 1000.
