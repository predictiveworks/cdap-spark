
# Count Vectorizer Builder

## Description

This machine learning plugin represents the building stage for an Apache Spark ML "Count Vectorizer model".

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Model Configuration
**Model Name**: The unique name of the machine learning model.

**Model Stage***: The stage of the ML model. Supported values are 'experiment', 'staging', 'production'
and 'archived'. Default is 'experiment'.

### Data Configuration
**Input Field**: The name of the field in the input schema that contains the features to build the model from.

### Parameter Configuration
**Vocabulary Size**: The maximum size of the vocabulary. If this value is smaller than the total number of 
different terms, the vocabulary will contain the top terms ordered by term frequency across the corpus.

**Minimum Document Frequency**: Specifies the minimum nonnegative number of different documents a term must 
appear in to be included in the vocabulary. Default is 1.

**Minimum Term Frequency**: Filter to ignore rare words in a document. For each document, terms with frequency 
(or count) less than the given threshold are ignored. Default is 1.