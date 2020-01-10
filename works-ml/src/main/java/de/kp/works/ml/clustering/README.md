# Clustering

## What is in it for you?

**Works-ML** externalizes the following clustering algorithms provided by Apache Spark ML:

* K-Means
* Latent Dirichlet Allocation

## K-Means

Machine learning can be categorized into three main categories:

* Supervised
* Unsupervised 
* Reinforcement

K-Means clustering is one of the simplest and popular **unsupervised** machine learning algorithms. Its goal is to find groups (clusters) in a certain dataset, with the number of clusters represented by the variable *k*. 

The algorithm iteratively allocates each data point to the nearest cluster based on its features. In every iteration, each point is assigned to its nearest cluster based on a specific distance metric, which is usually the Euclidean distance. 

The output of this clustering algorithm comprises the centroids of *k* clusters and the labeled dataset, i.e. each data record is labeled by the number of the nearest cluster.

K-Means can be used to identify unknown groups in complex and unlabeled datasets. Following are some business use cases of K-Means clustering:

* Customer segmentation based on purchase history
* Customer segmentation based on interest
* Insurance fraud detection
* Transaction fraud detection
* Detect unauthorized IoT devices based on network traffic
* Identity crime locality
* Group inventory by sales

## Latent Dirichlet Allocation