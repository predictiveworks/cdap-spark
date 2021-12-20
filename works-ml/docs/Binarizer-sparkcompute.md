
# Binarizer

## Description
This machine learning plugin represents the transformation stage that leverages the Apache Spark ML "Binarizer" 
to map continuous features onto binary values.

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Data Configuration
**Input Field**: The name of the field in the input schema that contains the features.

**Output Field**: The name of the field in the output schema that contains the transformed features.

### Parameter Configuration
**Threshold**: The nonnegative threshold used to binarize continuous features. The features greater 
than the threshold, will be binarized to 1.0. The features equal to or less than the threshold, will 
be binarized to 0.0. Default is 0.0.

