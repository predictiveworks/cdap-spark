
# Tokenizer

## Description
This machine learning plugin represents the transformation stage that leverages the Apache Spark ML "Regex Tokenizer"
to split an input text into a sequence of tokens.

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Data Configuration
**Input Field**: The name of the field in the input schema that contains the features.

**Output Field**: The name of the field in the output schema that contains the transformed features.

### Parameter Configuration
**Regex Pattern**: The regex pattern used to split the input text. The pattern is used to match delimiters, 
if 'gaps' = true or tokens if 'gaps' = false: Default is '\\s+'.		

**Token Length**: Minimum token length, greater than or equal to 0,  to avoid returning empty strings. 
Default is 1.

**Gaps**: Indicator to determine whether regex splits on gaps (true) or matches tokens (false). 
Default is 'true'.
