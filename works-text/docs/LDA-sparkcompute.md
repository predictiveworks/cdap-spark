
# LDA Text Predictor

## Description
This machine learning plugin represents a transformation stage to map text documents on their topic vectors, 
or most likely topic label. This stage is based on two trained models, an Apache Spark ML "LDA model", and a 
"Word Embedding model".

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Model Configuration
**Model Name**: The unique name of the machine learning model.

**Model Stage***: The stage of the ML model. Supported values are 'experiment', 'staging', 'production'
and 'archived'. Default is 'experiment'.

**Model Variant**: An indicator to determine which model variant is used for predictions. Supported values
are 'best' and 'latest'. Default is 'best'.

### Data Configuration
**Text Field**: The name of the field in the input schema that contains the text document.

**Topic Field**: The name of the field in the output schema that contains the prediction result.

### Parameter Configuration
**Embedding Name**: The unique name of the trained Word2Vec embedding model.

**Embedding Stage**: The stage of the Word2Vec embedding model. Supported values are 'experiment', 'staging',
'production' and 'archived'. Default is 'experiment'.

**Pooling Strategy**: The pooling strategy how to merge word embeddings into document embeddings. Supported 
values are 'average' and 'sum'. Default is 'average'.

**Topic Strategy**: The indicator to determine whether the trained LDA model is used to predict a topic label 
or vector. Supported values are 'label' & 'vector'. Default is 'vector'.
