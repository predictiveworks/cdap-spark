
# NGram

## Description
This machine learning plugin represents a transformation stage that leverages Apache Spark ML 
"N-Gram" transformer to convert the input array of string into an array of n-grams.

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Data Configuration
**Input Fields**: The comma-separated list of numeric (or numeric vector) fields that have to be assembled.

**Output Field**: The name of the field in the output schema that contains the transformed features.

### Parameter Configuration
**N-gram Length**: Minimum n-gram length, greater than or equal to 1. Default is 2.
