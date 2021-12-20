
# Gradient Boosted-Tree Classifier

## Description

This machine learning plugin represents the building stage for an Apache Spark ML "Gradient-Boosted Tree 
classification model". This stage expects a dataset with at least two fields to train the model: One as an 
array of numeric values, and, another that describes the class or label value as numeric value.

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Model Configuration
**Model Name**: The unique name of the machine learning model.

**Model Stage***: The stage of the ML model. Supported values are 'experiment', 'staging', 'production'
and 'archived'. Default is 'experiment'.

### Data Configuration
**Features Field**: The name of the field in the input schema that contains the feature vector.

**Label Field**: The name of the field in the input schema that contains the label.

**Data Split**: The split of the dataset into train & test data, e.g. 80:20. Default is 70:30.

### Parameter Configuration
**Loss Type**: The type of the loss function the Gradient-Boosted Trees algorithm tries to minimize. 
Default is 'logistic'.

**Minimum Gain**: The minimum information gain for a split to be considered at a tree node. The value should
be at least 0.0. Default is 0.0.

**Maximum Bins**: The maximum number of bins used for discretizing continuous features and for choosing how to
split on features at each node. More bins give higher granularity. Must be at least 2. Default is 32.

**Maximum Depth**: Nonnegative value that maximum depth of the tree. E.g. depth 0 means 1 leaf node;
depth 1 means 1 internal node + 2 leaf nodes. Default is 5.

**Maximum Iterations**: The maximum number of iterations to train the Gradient-Boosted Trees model. Default is 20.

**Learning Rate**: The learning rate for shrinking the contribution of each estimator. Must be in the interval (0, 1]. 
Default is 0.1.
