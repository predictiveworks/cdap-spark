
# ALS Predictor

## Description
This machine learning plugin represents the prediction stage that leverages a trained Apache Spark ML ALS 
recommendation model. ALS is short for "Alternating Least Squares" matrix factorization and defines an 
approved and often used recommendation algorithm. For more details, please refer to the "ALSSink".

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
**User Field**: The name of the input field that defines the "user" identifiers. The values must be within 
the integer value range.

**Item Field**: The name of the input field that defines the "item" identifiers. The values must be within 
the integer value range.

**Prediction Field**: The name of the field in the output schema that contains the predicted rating.
