
# Random Forest Classifier

## Description

This machine learning plugin represents the building stage

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
**Impurity**: Impurity is a criterion how to calculate information gain. Supported values: 'entropy'
and 'gini'. Default is 'gini'.

**Minimum Gain**: The minimum information gain for a split to be considered at a tree node. The value should
be at least 0.0. Default is 0.0.

**Maximum Bins**: The maximum number of bins used for discretizing continuous features and for choosing how to
split on features at each node. More bins give higher granularity. Must be at least 2. Default is 32.

**Maximum Depth**: Nonnegative value that maximum depth of the tree. E.g. depth 0 means 1 leaf node;
depth 1 means 1 internal node + 2 leaf nodes. Default is 5.

**Number of Trees**: The number of trees to train the model. Default is 20.