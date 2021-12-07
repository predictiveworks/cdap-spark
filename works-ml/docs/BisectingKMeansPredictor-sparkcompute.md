
# Bisecting KMeans Predictor

## Description
Bisecting KMeans is a kind of hierarchical clustering using a divisive (or “top-down”) approach:
all observations start in one cluster, and splits are performed recursively as one moves down the
hierarchy. This approach can often be much faster than regular KMeans, but it will generally produce
a different clustering.

This machine learning plugin represents the prediction stage that leverages a trained Apache Spark ML 
Bisecting K-Means clustering model to assign a certain cluster center to each data record.

## Configuration

### Model Configuration

### Data Configuration