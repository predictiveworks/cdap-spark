
# Norvig Checker

## Description
This machine learning plugin represents a transformation stage that checks the spelling of each normalized 
term in a text document, leveraging a trained Spark NLP Norvig Spelling model. This stage adds an extra field 
to the input schema that contains the whitespace separated set of suggested spelling corrections.

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Model Configuration
**Model Name**: The unique name of the machine learning model.

**Model Stage***: The stage of the ML model. Supported values are 'experiment', 'staging', 'production'
and 'archived'. Default is 'experiment'.

### Data Configuration
**Text Field**: The name of the field in the input schema that contains the text document.

**Suggestion Field**: The name of the field in the output schema that contains the suggested spellings.

### Parameter Configuration
**Probability Threshold**: The probability threshold above which a suggested term spelling is accepted. 
Default is 0.75.
