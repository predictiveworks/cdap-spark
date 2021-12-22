
# One Hot Encoder

## Description
This machine learning plugin represents a transformation stage to map input labels (indices) to binary vectors. 
This encoding allows algorithms which expect continuous features to use categorical features. This transformer 
expects a Numeric input and generates an Array[Double] as output.

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Data Configuration
**Input Fields**: The comma-separated list of numeric (or numeric vector) fields that have to be assembled.

**Output Field**: The name of the field in the output schema that contains the transformed features.

**Drop Last Category**: An indicator to specify whether to drop the last category in the encoder vectors. 
Default is 'true'.
