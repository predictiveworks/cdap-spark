# Feature Engineering

The feature engineering CDAP plugins are derived from [Apache Spark](https://spark.apache.org) v2.1.3 and made available 
for [CDAP](https://cdap.io) data pipelines.

## Binarizer

Binarization is the process of thresholding numerical features to binary (0/1) features. It is applied to an input 
record field that specifies an Array of numeric (Double, Float, Int, Long) values. 

Feature values greater than the threshold are binarized to 1.0; values equal to or less than the threshold are binarized to 0.0.

>Binarization is a pipeline stage that **does not** a trained feature model. 

## Bucketizer

Bucketizing is the process of transforming numerical features to feature buckets, where the buckets 
are defined by user-specific splits.

With n+1 splits, there are n buckets. A bucket defined by splits x, y holds values in the range [x, y) 
except the last bucket, which also includes y. Splits should be strictly increasing. 

Values at -infinity, infinity must be explicitly provided to cover all Double values; otherwise, values outside 
the splits specified will be treated as errors. 

>Bucketization is a pipeline stage that **does not** a trained feature model. 

## Counter Vectorizer

## Feature Normalizer

## Feature Scaling

Feature scaling is applied to an input record field that specifies an Array of numeric (Double, Float, Int, Long) values. The following scaler algorithms are supported:

### Max-Abs Scaler
### Min-Max Scaler
### Standard Scaler

## Locality Sensitive Hashing

## Principal Component Analysis

## Term Frequency Hashing

## Word2Vec Vectorizer