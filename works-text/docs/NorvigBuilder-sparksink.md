
# Norvig Builder

## Description
This machine learning plugin represents the building stage for a Spark NLP "Spell Checking model" based 
on Norvig's algorithm. The training corpus provides correctly spelled terms with one or multiple terms per 
document.

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Model Configuration
**Model Name**: The unique name of the machine learning model.

**Model Stage***: The stage of the ML model. Supported values are 'experiment', 'staging', 'production'
and 'archived'. Default is 'experiment'.

### Data Configuration
**Corpus Field**: The name of the field in the input schema that contains the correctly spelled tokens.

### Parameter Configuration
**Token Delimiter**: The delimiter to separate the tokens in the corpus.
