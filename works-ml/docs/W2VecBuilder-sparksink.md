
# Word2Vec Builder

## Description
This machine learning plugin represents the building stage for an Apache Spark ML "Word2Vec feature model".

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Model Configuration
**Model Name**: The unique name of the machine learning model.

**Model Stage***: The stage of the ML model. Supported values are 'experiment', 'staging', 'production'
and 'archived'. Default is 'experiment'.

### Data Configuration
**Input Field**: The name of the field in the input schema that contains the features to build the model from.

### Parameter Configuration
**Maximum Iterations**: The maximum number of iterations to train the Word-to-Vector model. Default is 1.

**Learning Rate**: The learning rate for shrinking the contribution of each estimator. Must be in the 
interval (0, 1]. Default is 0.025.

**Vector Size**: The positive dimension of the feature vector to represent a certain word. Default is 100.

**Window Size**: The positive window size. Default is 5.

**Minimum Word Frequency**: The minimum number of times a word must appear to be included in the 
Word-to-Vector vocabulary. Default is 5.

**Maximum Sentence Length**: The maximum length (in words) of each sentence in the input data. Any sentence 
longer than this threshold will be divided into chunks of this length. Default is 1000.
