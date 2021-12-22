
# MinHash LSH Builder

## Description
MinHash is an LSH family for Jaccard distance where input features are sets of natural numbers. 
This algorithm applies a random hash function g to each element in the set and take the minimum 
of all hashed values.

The input sets for MinHash are represented as binary vectors, where the vector indices represent 
the elements themselves, and the non-zero values in the vector represent the presence of that element 
in the set. While both dense and sparse vectors are supported, typically sparse vectors are recommended 
for efficiency. 

This machine learning plugin represents a building stage for an Apache Spark ML "MinHash LSH model".

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Model Configuration
**Model Name**: The unique name of the machine learning model.

**Model Stage***: The stage of the ML model. Supported values are 'experiment', 'staging', 'production'
and 'archived'. Default is 'experiment'.

### Data Configuration
**Input Field**: The name of the field in the input schema that contains the features to build the model from.

### Parameter Configuration
**Number of Hash Tables**: The number of hash tables used in LSH OR-amplification. LSH OR-amplification can be 
used to reduce the false negative rate. Higher values for this parameter lead to a reduced false negative rate, 
at the expense of added computational complexity. Default is 1.