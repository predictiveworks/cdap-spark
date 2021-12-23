
# Sentiment Builder

## Description
This machine learning plugin represents the building stage for a Spark NLP "Sentiment Analysis model" based 
on the sentiment algorithm introduced by Vivek Narayanan. The training corpus comprises a labeled set of 
sentiment tokens.

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Model Configuration
**Model Name**: The unique name of the machine learning model.

**Model Stage***: The stage of the ML model. Supported values are 'experiment', 'staging', 'production'
and 'archived'. Default is 'experiment'.

### Data Configuration
**Corpus Field**: The name of the field in the input schema that contains the labeled sentiment tokens.

**Data Split**: The split of the dataset into train & test data, e.g. 80:20. Default is 70:30.

### Parameter Configuration
**Sentiment Delimiter**: The delimiter to separate labels and associated tokens in the corpus. 
Default is '->'.
