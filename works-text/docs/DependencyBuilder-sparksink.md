
# Word Dependency Builder

## Description
This machine learning plugin represents the building stage for a Spark NLP "Unlabeled Dependency 
Parser model".

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Model Configuration
**Model Name**: The unique name of the machine learning model.

**Model Stage***: The stage of the ML model. Supported values are 'experiment', 'staging', 'production'
and 'archived'. Default is 'experiment'.

### Data Configuration
**Corpus Field**: The name of the field in the input schema that contains the annotated corpus document.

### Parameter Configuration
**Corpus Format**: The format of the training corpus. Supported values are 'conll-u' (CoNLL-U corpus) 
and 'treebank' (TreeBank corpus). Default is 'conll-u'.

**Iterations**: The number of iterations to train the model. Default is 10.
