
# SMOTE Sampler

## Description
This machine learning plugin represents a preparation stage for either building Apache Spark based classification or 
regression models. This stage leverages the SMOTE algorithm to extends a training dataset containing features & labels
with synthetic data records.

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Data Configuration
**Features Field**: The name of the field in the input schema that contains the feature vector.

**Label Field**: The name of the field in the input schema that contains the label.

### Parameter Configuration
**Number of Hash Tables**: The number of hash tables used in LSH OR-amplification. LSH OR-amplification can be used 
to reduce the false negative rate. Higher values for this parameter lead to a reduced false negative rate, at the 
expense of added computational complexity. Default is 1.

**Bucket Length**: The length of each hash bucket, a larger bucket lowers the false negative rate. The number of 
buckets will be '(max L2 norm of input vectors) / bucketLength'.

**Nearest Neighbors**: The number of nearest neighbors that are taken into account by the SMOTE algorithm to interpolate 
synthetic feature values. Default is 4.
