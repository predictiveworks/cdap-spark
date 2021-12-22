
# Decision Tree Predictor

## Description
This machine learning plugin represents the prediction stage that leverages a trained Apache Spark ML "Decision 
Tree classifier model" or "Decision Tree regressor model". The model type parameter determines whether this stage 
predicts from a classifier or regressor model.

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Model Configuration
**Model Name**: The unique name of the machine learning model.

**Model Stage***: The stage of the ML model. Supported values are 'experiment', 'staging', 'production'
and 'archived'. Default is 'experiment'.

**Model Variant**: An indicator to determine which model variant is used for predictions. Supported values
are 'best' and 'latest'. Default is 'best'. The best model refers to the model with the highest accuracy
with respect to all other (model) training runs.

**Model Type**: The type of the model that is used for prediction, either 'classifier' or 'regressor'.

### Data Configuration
**Features Field**: The name of the field in the input schema that contains the feature vector.

**Prediction Field**: The name of the field in the output schema that contains the predicted label.