
# Vector Assembler

## Description
This machine learning plugin represents the transformation stage that leverages the Apache Spark ML "Vector Assembler"
to merge multiple numeric (or numeric vector) fields into a single feature vector.

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Data Configuration
**Input Fields**: The comma-separated list of numeric (or numeric vector) fields that have to be assembled.

**Output Field**: The name of the field in the output schema that contains the transformed features.