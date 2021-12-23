
# NER Tagger

## Description
This machine learning plugin represents a tagging stage that leverages a trained Spark NLP "Word2Vec model" 
and "NER (CRF) model" to map an input text field onto an output token & entities field.

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Model Configuration
**Model Name**: The unique name of the machine learning model.

**Model Stage***: The stage of the ML model. Supported values are 'experiment', 'staging', 'production'
and 'archived'. Default is 'experiment'.

### Data Configuration
**Text Field**: The name of the field in the input schema that contains the document.

**Token Field**: The name of the field in the output schema that contains the extracted tokens

**Entities Field**: The name of the field in the output schema that contains the extracted entities.

### Parameter Configuration
**Embedding Name**: The unique name of the trained Word2Vec embedding model.

**Embedding Stage**: The stage of the Word2Vec embedding model. Supported values are 'experiment', 'staging',
'production' and 'archived'. Default is 'experiment'.

**Normalization**: The indicator to determine whether token normalization has to be applied. Normalization 
restricts the characters of a token to [A-Za-z0-9-]. Supported values are 'true' and 'false'. Default is 'true'.
