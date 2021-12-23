
# Topic Predictor

## Description
This machine learning plugin represents a transformation stage to either determine the topic-distribution per 
document or term-distribution per topic. This stage is based on a trained Apache Spark ML "Topic model".

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Model Configuration
**Model Name**: The unique name of the machine learning model.

**Model Stage***: The stage of the ML model. Supported values are 'experiment', 'staging', 'production'
and 'archived'. Default is 'experiment'.

**Model Variant**: An indicator to determine which model variant is used for predictions. Supported values
are 'best' and 'latest'. Default is 'best'.

### Data Configuration
**Text Field**: The name of the field in the input schema that contains the text document.

### Parameter Configuration
**Topic Strategy**: The indicator to determine whether to retrieve document-topic or topic-term description. 
Supported values are 'document-topic' and 'topic-term'. Default is 'document-topic'.