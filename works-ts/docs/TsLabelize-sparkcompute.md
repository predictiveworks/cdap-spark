
# TS Labelize

## Description

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Data Configuration
**Time Field**: The name of the field in the input schema that contains the time value.

**Value Field**: The name of the field in the input schema that contains the value.

**Features Field**: The name of the field in the output schema that contains the feature vector.

**Label Field**: The name of the field in the output schema that contains the label value.

### Parameter Configuration
**Feature Dimensions**: The dimension of the feature vector, i.e the number of past observations in 
time that are assembled as a vector. Default is 10.