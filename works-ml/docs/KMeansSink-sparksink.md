
# KMeans Clustering
Clustering is an unsupervised learning problem whereby we aim to group subsets of entities with one
another based on some notion of similarity. Clustering is often used for exploratory analysis and/or
as a component of a hierarchical supervised learning pipeline, in which distinct classifiers or regression
models are trained for each cluster.

## Description
KMeans is one of the most commonly used clustering algorithms that clusters the data points into 
a predefined number of clusters. This implementation includes a parallelized variant of the KMeans
method that is used by default.

This machine learning plugin represents the building stage for an Apache Spark ML K-Means clustering 
model. This stage expects a dataset with at least one 'features' field as an array of numeric values to 
train the model.

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Model Configuration
**Model Name**: The unique name of the machine learning model.

**Model Stage***: The stage of the ML model. Supported values are 'experiment', 'staging', 'production'
and 'archived'. Default is 'experiment'.

### Data Configuration
**Features Field**: The name of the field in the input schema that contains the feature vector.

### Parameter Configuration
