
# Token Cleaner

## Description
This machine learning plugin represents a transformation stage that leverages the Spark NLP "Stop Word Cleaner"
to map an input text field onto its normalized terms and remove each term that is defined as stop word. 

This stage adds an extra field to the input schema that contains the whitespace separated set of remaining tokens.

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Data Configuration
**Text Field**: The name of the field in the input schema that contains the text document.

**Cleaned Field**: The name of the field in the output schema that contains the cleaned tokens.

### Parameter Configuration
**Stop Words**: A delimiter separated list of stop words, i.e. words that have to be removed from the 
extracted tokens.

**Word Delimiter**: The delimiter used to separate the different stop words. Default is comma-separated.
