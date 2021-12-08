
# Bisecting KMeans Clustering
Clustering is an unsupervised learning problem whereby we aim to group subsets of entities with one 
another based on some notion of similarity. Clustering is often used for exploratory analysis and/or 
as a component of a hierarchical supervised learning pipeline, in which distinct classifiers or regression 
models are trained for each cluster.

## Description
Bisecting KMeans is a kind of hierarchical clustering using a divisive (or “top-down”) approach: 
all observations start in one cluster, and splits are performed recursively as one moves down the 
hierarchy. This approach can often be much faster than regular KMeans, but it will generally produce 
a different clustering.

This machine learning plugin represents the building stage for an Apache Spark ML Bisecting K-Means 
clustering model. It expects a dataset with at least one 'features' field as an array of numeric values 
to train the model.

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Model Configuration
**Model Name**: The unique name of the machine learning model.

**Model Stage***: The stage of the ML model. Supported values are 'experiment', 'staging', 'production'
and 'archived'. Default is 'experiment'.

### Data Configuration
**Features Field**: The name of the field in the input schema that contains the feature vector.

### Parameter Configuration
