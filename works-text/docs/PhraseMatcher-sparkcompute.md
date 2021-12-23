
# Phrase Matcher

## Description
This machine learning plugin represents a transformation stage that leverages the Spark NLP "Text Matcher" 
to detected provided phrases in the input text document.

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Data Configuration
**Text Field**: The name of the field in the input schema that contains the text document.

**Phrase Field**: The name of the field in the output schema that contains the text matches.

### Parameter Configuration
**Phrases**: A delimiter separated list of text phrases.

**Phrase Delimiter**: The delimiter used to separate the different text phrases.
