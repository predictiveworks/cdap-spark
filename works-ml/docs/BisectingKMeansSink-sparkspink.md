
# Bisecting KMeans

## Description
Bisecting KMeans is a kind of hierarchical clustering using a divisive (or “top-down”) approach: 
all observations start in one cluster, and splits are performed recursively as one moves down the 
hierarchy. This approach can often be much faster than regular KMeans, but it will generally produce 
a different clustering.

This machine learning plugin represents the building stage for an Apache Spark ML Bisecting K-Means 
clustering model. It expects a dataset with at least one feature field as an array of numeric values 
to train the model.

## Configuration

### Model Configuration

### Data Configuration

### Parameter Configuration
