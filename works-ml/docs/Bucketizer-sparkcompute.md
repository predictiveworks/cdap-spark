
# Bucketizer

## Description
Bucketizer transforms a column of continuous features to a column of feature buckets, where the buckets 
are specified by users. It takes a parameter: splits.
 
With n+1 splits, there are n buckets. A bucket defined by splits x, y holds values in the range [x,y) 
except the last bucket, which also includes y. Splits should be strictly increasing. 

Values at -inf, inf must be explicitly provided to cover all Double values; Otherwise, values outside 
the splits specified will be treated as errors. 
 
Two examples of splits are Array(Double.NegativeInfinity, 0.0, 1.0, Double.PositiveInfinity) and 
Array(0.0, 1.0, 2.0). Note that if you have no idea of the upper and lower bounds of the targeted column, 
you should add *Double.NegativeInfinity* and *Double.PositiveInfinity* as the bounds of your splits to prevent 
a potential out of Bucketizer bounds exception.

Note also that the splits that you provided have to be in strictly increasing order, i.e. s0 < s1 < s2 < ... < sn.

This machine learning plugin represents the transformation stage that leverages the Apache Spark ML 
"Feature Bucketizer" to map continuous features onto feature buckets.

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Data Configuration
**Input Field**: The name of the field in the input schema that contains the features.

**Output Field**: The name of the field in the output schema that contains the transformed features.

**Splits**: A comma separated list of split points (Double values) for mapping continuous features into buckets.
With n+1 splits, there are n buckets. A bucket defined by splits x,y holds values in the range [x,y) except the 
last bucket, which also includes y. 

The splits should be of length >= 3 and strictly increasing. Values at -infinity, infinity must be explicitly 
provided to cover all Double values; otherwise, values outside the splits specified will be treated as errors.


