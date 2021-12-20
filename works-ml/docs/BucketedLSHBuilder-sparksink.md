
# Bucketed LSH Builder

## Description
Bucketed Random Projection is an LSH family for Euclidean distance. Its LSH family projects 
feature vectors x onto a random unit vector v and portions the projected results into hash buckets:
```
 			h(x) = (x * v) / r
```
where r is a user-defined bucket length. The bucket length can be used to control the average size 
of hash buckets (and thus the number of buckets). 

A larger bucket length (i.e., fewer buckets) increases the probability of features being hashed to 
the same bucket (increasing the numbers of true and false positives).

Bucketed Random Projection accepts arbitrary vectors as input features, and supports both sparse and 
dense vectors.

This machine learning plugin represents the building stage for an Apache Spark ML "Bucketed Random Projection 
LSH model".

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Model Configuration
**Model Name**: The unique name of the machine learning model.

**Model Stage***: The stage of the ML model. Supported values are 'experiment', 'staging', 'production'
and 'archived'. Default is 'experiment'.

### Data Configuration
**Input Field**: The name of the field in the input schema that contains the features.

### Parameter Configuration
**Number of Hash Tables**: The number of hash tables used in LSH OR-amplification. LSH OR-amplification 
can be used to reduce the false negative rate. Higher values for this parameter lead to a reduced false 
negative rate, at the expense of added computational complexity. Default is 1.

**Bucket Length**: The length of each hash bucket, a larger bucket lowers the false negative rate. 
The number of buckets will be "(max L2 norm of input vectors) / bucketLength".
