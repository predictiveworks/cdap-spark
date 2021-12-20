
# Survival Predictor

## Description
This machine learning plugin represents the prediction stage

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Model Configuration
**Model Name**: The unique name of the machine learning model.

**Model Stage***: The stage of the ML model. Supported values are 'experiment', 'staging', 'production'
and 'archived'. Default is 'experiment'.

**Model Variant**: An indicator to determine which model variant is used for predictions. Supported values
are 'best' and 'latest'. Default is 'best'. The best model refers to the model with the highest accuracy
with respect to all other (model) training runs.

### Data Configuration
**Features Field**: The name of the field in the input schema that contains the feature vector.

**Prediction Field**: The name of the field in the output schema that contains the predicted label.
