
# Latent Dirichlet Clustering
Clustering is an unsupervised learning problem whereby we aim to group subsets of entities with one
another based on some notion of similarity. Clustering is often used for exploratory analysis and/or
as a component of a hierarchical supervised learning pipeline, in which distinct classifiers or regression
models are trained for each cluster.

## Description
Latent Dirichlet allocation (LDA) is a topic model which infers topics from a collection of text documents. 
LDA can be thought of as a clustering algorithm as follows:

* Topics correspond to cluster centers, and documents correspond to rows in a dataset.
* Topics and documents both exist in a feature space, where feature vectors are vectors 
  of word counts (bag of words).
* Rather than estimating a clustering using a traditional distance, LDA uses a function 
  based on a statistical model of how text documents are generated.
  
LDA supports different inference algorithms. This implementation, however, is restricted to the 
*Expectation-Maximization* optimizer on the likelihood function and yields comprehensive results.

This machine learning plugin represents the building stage for an Apache Spark ML Latent Dirichlet 
Allocation (LDA) clustering model. This stage expects a dataset with at least one 'features' field 
as an array of numeric values to train the model.

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Model Configuration
**Model Name**: The unique name of the machine learning model.

**Model Stage***: The stage of the ML model. Supported values are 'experiment', 'staging', 'production'
and 'archived'. Default is 'experiment'.

### Data Configuration
**Features Field**: The name of the field in the input schema that contains the feature vector.

**Data Split**: The split of the dataset into train & test data, e.g. 80:20. Default is 90:10.

### Parameter Configuration
**Clusters**: The number of topics that have to be created. Default is 10.

**Maximum Iterations**: The (maximum) number of iterations the algorithm has to execute. Default value is 20.