
# N-Gram Generator

## Description
This machine learning plugin represents a transformation stage that leverages the Spark NLP "N-gram generator" 
to map an input text field onto an output field that contains its associated N-grams.

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Data Configuration
**Text Field**: The name of the field in the input schema that contains the text document.

**N-Gram Field**: The name of the field in the output schema that contains the N-grams.

### Parameter Configuration
**N-Gram Length**: Minimum n-gram length, greater than or equal to 1. Default is 2.
