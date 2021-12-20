
# Multi-Layer Perceptron Classifier

## Description

This machine learning plugin represents the building stage for an Apache Spark ML "Multi-Layer Perceptron classification
model". This stage expects a dataset with at least two fields to train the model: One as an array of numeric values, and, 
another that describes the class or label value as numeric value.

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
**Solver Algorithm**: The solver algorithm for optimization. Supported options are 'gd' (mini-batch gradient 
descent) and 'l-bfgs'. Default is 'l-bfgs'.

**Layer Sizes**: The comma-separated list of the sizes of the layers from the input to the output layer.
For example: 780,100,10 means 780 inputs, one hidden layer with 100 neurons, and an output layer with 10 
neurons. At least 2 layers (input, output) must be specified.

**Block Size**: The nonnegative block size for stacking input data in matrices to speed up the computation.
Data is stacked within partitions. If block size is more than remaining data in a partition then it is adjusted 
to the size of this data. Recommended size is between 10 and 1000. Default is 128.

**Maximum Iterations**: The maximum number of iterations to train the Multi-Layer Perceptron model. Default is 100.

**Learning Rate**: The learning rate for shrinking the contribution of each estimator. Must be in the interval (0, 1]. 
Default is 0.03.

**Conversion Tolerance**: The positive convergence tolerance of iterations. Smaller values will lead to higher accuracy 
with the cost of more iterations. Default is 1e-6.
