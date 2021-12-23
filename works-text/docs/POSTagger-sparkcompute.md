
# POS Tagger

## Description
This machine learning plugin represents a transformation stage that requires a trained Spark NLP "Part-of-Speech model". 
This stage appends two fields to the input schema, one that contains the extracted terms per document, and another 
that contains their POS tags.

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Model Configuration
**Model Name**: The unique name of the machine learning model.

**Model Stage***: The stage of the ML model. Supported values are 'experiment', 'staging', 'production'
and 'archived'. Default is 'experiment'.

### Data Configuration
**Text Field**: The name of the field in the input schema that contains the text document.

**Output Field**: The name of the field in the output schema that contains the mixin of extracted tokens and 
predicted POS tags.
