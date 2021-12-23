
# Regex Matcher

## Description
This machine learning plugin represents a transformation stage that leverages the Spark NLP "Regex Matcher"
to detect provided Regex rules in the input text document.

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Data Configuration
**Text Field**: The name of the field in the input schema that contains the text document.

**Token Field**: The name of the field in the output schema that contains the text matches.

### Parameter Configuration
**Regex Rules**: A delimiter separated list of Regex rules.

**Rule Delimiter**: The delimiter used to separate the different Regex rules.