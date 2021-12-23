
# NER Relation

## Description
This machine learning plugin represents a transformation stage that leverages the result of an NER tagging stage
and extracts relations between named entities from their co-occurring.

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Data Configuration
**Token Field**: The name of the field in the input schema that contains the extracted tokens.

**Entities Field**: The name of the field in the input schema that contains the extracted named entity tags.