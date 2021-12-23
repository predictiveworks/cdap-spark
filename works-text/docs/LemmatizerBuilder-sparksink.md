
# Lemmatizer Builder

## Description
This machine learning plugin represents the building stage for a Spark NLP "Lemmatization model". The training 
corpus assigns each lemma to a set of term variations that all map onto this lemma.

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Model Configuration
**Model Name**: The unique name of the machine learning model.

**Model Stage***: The stage of the ML model. Supported values are 'experiment', 'staging', 'production'
and 'archived'. Default is 'experiment'.

### Data Configuration
**Corpus Field**: The name of the field in the input schema that contains the lemma and assigned tokens.

### Parameter Configuration
**Lemma Delimiter**: The delimiter to separate lemma and associated tokens in the corpus. Key & value 
delimiter must be different.

**Token Delimiter**: The delimiter to separate the tokens in the corpus. Key & value delimiter must be 
different.