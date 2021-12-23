
# Token Normalizer

## Description
This machine learning plugin represents a transformation stage that leverages the Spark NLP "Normalizer" 
to map an input text field onto an output field that contains normalized tokens. 

The Normalizer will clean up each token, taking as input column token out from the Tokenizer, and putting 
normalized tokens in the normal column. Cleaning up includes removing any non-character strings.

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Data Configuration
**Text Field**: The name of the field in the input schema that contains the text document.

**Norm-Token Field**: The name of the field in the output schema that contains the normalized tokens.