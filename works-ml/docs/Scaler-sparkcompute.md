
# Feature Scaler

## Description
This machine learning plugin represents a transformation stage that leverages a trained Scaler model 
to project feature vectors onto scaled vectors. Supported models are 'Max-Abs', 'Min-Max' and 'Standard' Scaler.

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Model Configuration
**Model Name**: The unique name of the machine learning model.

**Model Stage***: The stage of the ML model. Supported values are 'experiment', 'staging', 'production'
and 'archived'. Default is 'experiment'.

**Model Type**: The type of the scaler model. Supported values are 'maxabs', 'minmax' and 'standard'. 
Default is 'standard'.

### Data Configuration
**Input Field**: The name of the field in the input schema that contains the features.

**Output Field**: The name of the field in the output schema that contains the transformed features.
