
# TF IDF Vectorizer

## Description
This machine learning plugin represents a transformation stage that leverages an Apache Spark "ML TF-IDF feature model"
to map a sequence of words into its feature vector. Term frequency-inverse document frequency (TF-IDF) is a feature 
vectorization method widely used in text mining to reflect the importance of a term to a document in the corpus.

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Model Configuration
**Model Name**: The unique name of the machine learning model.

**Model Stage***: The stage of the ML model. Supported values are 'experiment', 'staging', 'production'
and 'archived'. Default is 'experiment'.

### Data Configuration
**Input Field**: The name of the field in the input schema that contains the features.

**Output Field**: The name of the field in the output schema that contains the transformed features.
