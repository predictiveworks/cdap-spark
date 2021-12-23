
# Word2Vec Builder

## Description
This machine learning plugin represents the building stage for an Apache Spark NLP "Word2Vec Embedding model".

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Model Configuration
**Model Name**: The unique name of the machine learning model.

**Model Stage***: The stage of the ML model. Supported values are 'experiment', 'staging', 'production'
and 'archived'. Default is 'experiment'.

### Data Configuration
**Text Field**: The name of the field in the input schema that contains the text document.

### Parameter Configuration
**Normalization**: The indicator to determine whether token normalization has to be applied. Normalization 
restricts the characters of a token to [A-Za-z0-9-]. Supported values are 'true' and 'false'. Default is 
'true'.

**Maximum Iterations**: The maximum number of iterations to train the Word2Vec model. Default is 1.

**Learning Rate**: The learning rate for shrinking the contribution of each estimator. Must be in the 
interval (0, 1]. Default is 0.025.

**Vector Size**: The positive dimension of the feature vector to represent a certain word. Default is 100.

**Window Size**: The positive window size. Default is 5.

**Minimum Word Frequency**: The minimum number of times a word must appear to be included in the Word2Vec 
vocabulary. Default is 5.

**Maximum Sentence Length**: The maximum length (in words) of each sentence in the input data. Any sentence 
longer than this threshold will be divided into chunks of this length. Default is 1000.