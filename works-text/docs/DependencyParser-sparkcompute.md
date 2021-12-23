
# Word Dependency Parser

## Description
This machine learning plugin represents a transformation stage that leverages a Spark NLP "Unlabeled Dependency 
Parser model" to extract syntactic relations between words in a text document.

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Model Configuration
**Model Name**: The unique name of the machine learning model.

**Model Stage***: The stage of the ML model. Supported values are 'experiment', 'staging', 'production'
and 'archived'. Default is 'experiment'.

### Data Configuration
**Text Field**: The name of the field in the input schema that contains the text document.

**Sentence Field**: The name of the field in the output schema that contains the extracted sentences.

**Dependency Field**: The name of the field in the output schema that contains the word dependencies.

### Parameter Configuration
**Part-of-Speech Name**: The unique name of trained Part of Speech model.

**Part-of-Speech Stage**: The stage of the Part of Speech model. Supported values are 'experiment', 'staging', 
'production' and 'archived'. Default is 'experiment'.