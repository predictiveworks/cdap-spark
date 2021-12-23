
# POS Builder

## Description
This machine learning plugin represents the building stage for a Spark NLP "Part of Speech model".

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Model Configuration
**Model Name**: The unique name of the machine learning model.

**Model Stage***: The stage of the ML model. Supported values are 'experiment', 'staging', 'production'
and 'archived'. Default is 'experiment'.

### Data Configuration
**Corpus Field**: Name of the input text field that contains the annotated sentences for training purpose.

### Parameter Configuration
**Delimiter**: The delimiter in the input text line to separate tokens and POS tags. Default is '|'.

**Maximum Iterations**: The maximum number of iterations to train the Part-of-Speech model. Default is 5.
