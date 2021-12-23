
# Lemmatizer

## Description
This machine learning plugin represents a transformation stage requires a trained Spark NLP "Lemmatization model."
It extracts normalized terms from a text document and maps each term onto its trained lemma. This stage adds an 
extra field to the input schema that contains the whitespace separated set of lemmas.

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Model Configuration
**Model Name**: The unique name of the machine learning model.

**Model Stage***: The stage of the ML model. Supported values are 'experiment', 'staging', 'production'
and 'archived'. Default is 'experiment'.

### Data Configuration
**Text Field**: The name of the field in the input schema that contains the text document.

**Lemma Field**: The name of the field in the output schema that contains the detected lemmas.
