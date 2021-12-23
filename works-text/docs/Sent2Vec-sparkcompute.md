
# Sent2Vec Embedding

## Description
This machine learning plugin represents an embedding stage that leverages a trained Spark NLP "Word2Vec 
Embedding model" to map an input text field onto an output sentence & sentence embedding field with a 
user-specific pooling strategy.

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Model Configuration
**Model Name**: The unique name of the machine learning model.

**Model Stage***: The stage of the ML model. Supported values are 'experiment', 'staging', 'production'
and 'archived'. Default is 'experiment'.

### Data Configuration
**Text Field**: The name of the field in the input schema that contains the text document.

**Sentence Field**: The name of the field in the output schema that contains the extracted sentences.

**Embedding Field**: The name of the field in the output schema that contains the sentence embeddings.

### Parameter Configuration
**Normalization**: The indicator to determine whether token normalization has to be applied. Normalization
restricts the characters of a token to [A-Za-z0-9-]. Supported values are 'true' and 'false'. Default is
'true'.

**Pooling Strategy**: The pooling strategy how to merge word embeddings into sentence embeddings. Supported 
values are 'average' and 'sum'. Default is 'average'.
