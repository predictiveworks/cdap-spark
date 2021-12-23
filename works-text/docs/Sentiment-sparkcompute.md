
# Sentiment

## Description
This machine learning plugin represents a transformation stage that predicts sentiment labels (positive or negative) 
for text documents, leveraging a trained Spark NLP "Sentiment Analysis model".

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Model Configuration
**Model Name**: The unique name of the machine learning model.

**Model Stage***: The stage of the ML model. Supported values are 'experiment', 'staging', 'production'
and 'archived'. Default is 'experiment'.

**Model Variant**: An indicator to determine which model variant is used for predictions. Supported values 
are 'best' and 'latest'. Default is 'best'.

### Data Configuration
**Text Field**: The name of the field in the input schema that contains the document.

**Prediction Field**: The name of the field in the output schema that contains the predicted sentiment.
