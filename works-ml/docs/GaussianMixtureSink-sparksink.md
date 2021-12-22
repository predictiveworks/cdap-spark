
# Gaussian Mixture Clustering
Clustering is an unsupervised learning problem whereby we aim to group subsets of entities with one
another based on some notion of similarity. Clustering is often used for exploratory analysis and/or
as a component of a hierarchical supervised learning pipeline, in which distinct classifiers or regression
models are trained for each cluster.

## Description
A Gaussian Mixture Model represents a composite distribution whereby points are drawn from one of 
*k* Gaussian sub-distributions, each with its own probability. This implementation uses the 
*Expectation-Maximization* algorithm to induce the maximum-likelihood model given a set of samples.

This machine learning plugin represents the building stage for an Apache Spark ML Gaussian Mixture 
clustering model. This stage expects a dataset with at least one 'features' field as an array of 
numeric values to train the model.

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Model Configuration
**Model Name**: The unique name of the machine learning model.

**Model Stage***: The stage of the ML model. Supported values are 'experiment', 'staging', 'production'
and 'archived'. Default is 'experiment'.

### Data Configuration
**Features Field**: The name of the field in the input schema that contains the feature vector.

### Parameter Configuration
**Clusters**: The number of independent Gaussian distributions in the dataset. Must be > 1. Default is 2.

**Maximum Iterations**: The (maximum) number of iterations the algorithm has to execute. Default value is 100.

**Conversion Tolerance**: The positive convergence tolerance of iterations. Smaller values will lead to higher 
accuracy with the cost of more iterations. Default is 0.01.
