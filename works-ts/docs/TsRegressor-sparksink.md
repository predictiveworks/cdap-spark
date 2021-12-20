
# TS Regressor

## Description

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

## Model Configuration
**Model Name**: The unique name of the machine learning model.

**Model Stage***: The stage of the ML model. Supported values are 'experiment', 'staging', 'production'
and 'archived'. Default is 'experiment'.

### Data Configuration
**Time Field**: The name of the field in the input schema that contains the time value.

**Value Field**: The name of the field in the input schema that contains the value.

**Time Split**: The split of the dataset into train & test data, e.g. 80:20. Note, this is a split time
and is computed from the total time span (min, max) of the time series. Default is 70:30.

### Parameter Configuration
**Lag Order**: The positive number of past points of time to take into account for vectorization. 
Default is 20.

**Minimum Gain**: The minimum information gain for a split to be considered at a tree node. The value 
should be at least 0.0. Default is 0.0.

**Maximum Bins**: The maximum number of bins used for discretizing continuous features and for choosing 
how to split on features at each node. More bins give higher granularity. Must be at least 2. Default is 32.

**Maximum Depth**: Nonnegative value that maximum depth of the tree. E.g. depth 0 means 1 leaf node; depth 1 
means 1 internal node + 2 leaf nodes. Default is 5.

**Number of Trees**: The number of trees to train the model. Default is 20.