
# ALS Predictor

## Description
This machine learning plugin represents the prediction stage that leverages a trained Apache Spark ML ALS 
recommendation model. For more details, please refer to the "ALSSink".

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Model Configuration
**Model Name**: The unique name of the machine learning model.

**Model Stage***: The stage of the ML model. Supported values are 'experiment', 'staging', 'production'
and 'archived'. Default is 'experiment'.

**Model Variant**:

### Data Configuration
**User Field**:

**Item Field**:

**Prediction Field**:
