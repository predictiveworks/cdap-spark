
# Normalizer

## Description
This machine learning plugin represents a transformation stage to normalize a feature vector to have unit 
norm using the given p-norm.

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Data Configuration
**Input Fields**: The comma-separated list of numeric (or numeric vector) fields that have to be assembled.

**Output Field**: The name of the field in the output schema that contains the transformed features.

### Parameter Configuration
**P-Norm**: The p-norm to use for normalization. Supported values are '1' and '2'. Default is 2.
