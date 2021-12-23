
# Document Assembler

## Description
This machine learning plugin represents a transformation stage that leverages the Spark NLP "Document Assembler"
to map an input text field into a document field, preparing it for further NLP annotations.

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Data Configuration
**Text Field**: The name of the field in the input schema that contains the text document.

**Document Field**: The name of the field in the output schema that contains the document annotations.
