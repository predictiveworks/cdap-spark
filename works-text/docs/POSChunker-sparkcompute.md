
# POS Chunker

## Description
This machine learning plugin represents a transformation stage that extracts meaningful phrases from text 
documents. Phrase extraction is based on patterns of part-of-speech tags. This stage requires a trained 
Spark NLP "Part-of-Speech model".

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Model Configuration
**Model Name**: The unique name of the machine learning model.

**Model Stage***: The stage of the ML model. Supported values are 'experiment', 'staging', 'production'
and 'archived'. Default is 'experiment'.

### Data Configuration
**Text Field**: The name of the field in the input schema that contains the text document.

**Token Field**:

**Chunk Field**: The name of the field in the output schema that contains the extracted chunks.

### Parameter Configuration
**Regex Rules**: A delimiter separated list of chunking rules.

**Rule Delimiter**: The delimiter used to separate the different chunking rules.
