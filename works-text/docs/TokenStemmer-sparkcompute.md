
# Token Stemmer

## Description
This machine learning plugin represents a transformation stage that leverages the Spark NLP "Stemmer" 
to map an input text field onto its normalized terms and reduce each terms to its linguistic stem.

This stage adds an extra field to the input schema that contains the whitespace separated set of stems.

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Data Configuration
**Text Field**: The name of the field in the input schema that contains the text document.

**Stem Field**: The name of the field in the output schema that contains the stemmed tokens.
