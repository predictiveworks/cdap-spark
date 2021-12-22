
# Word2Vec Vectorizer

## Description
This machine learning plugin represents the transformation stage that turns a sentence into a vector to 
represent the whole sentence. The transform is performed by averaging all word vectors it contains, based 
on a trained Apache Spark ML "Word2Vec feature model".

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Model Configuration
**Model Name**: The unique name of the machine learning model.

**Model Stage***: The stage of the ML model. Supported values are 'experiment', 'staging', 'production'
and 'archived'. Default is 'experiment'.

### Data Configuration
**Input Field**: The name of the field in the input schema that contains the features.

**Output Field**: The name of the field in the output schema that contains the transformed features.