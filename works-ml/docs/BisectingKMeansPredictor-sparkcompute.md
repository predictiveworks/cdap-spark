
# Bisecting KMeans Predictor

## Description
Bisecting KMeans is a kind of hierarchical clustering using a divisive (or “top-down”) approach:
all observations start in one cluster, and splits are performed recursively as one moves down the
hierarchy. This approach can often be much faster than regular KMeans, but it will generally produce
a different clustering.

This machine learning plugin represents the prediction stage that leverages a trained Apache Spark ML 
"Bisecting K-Means clustering model" to assign a certain cluster center to each data record.

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