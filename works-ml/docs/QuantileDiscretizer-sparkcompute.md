
# Quantile Discretizer

## Description
'QuantileDiscretizer' takes a column with continuous features and outputs a
column with binned categorical features. The number of bins can be set using
the `numBuckets` parameter. It is possible that the number of buckets used
will be smaller than this value, for example, if there are too few distinct
values of the input to create enough distinct quantiles.

**NaN handling**: NaN values will be removed from the column during
`QuantileDiscretizer` fitting. This will produce a `Bucketizer` model for
making predictions. During the transformation, `Bucketizer` will raise an
error when it finds NaN values in the dataset, but the user can also choose
to either keep or remove NaN values within the dataset by setting
`handleInvalid`. If the user chooses to keep NaN values, they will be handled
specially and placed into their own bucket, for example, if 4 buckets are
used, then non-NaN data will be put into buckets[0-3], but NaNs will be
counted in a special bucket[4].

**Algorithm**: The bin ranges are chosen to use an approximate algorithm. 
The precision of the approximation can be controlled with the `relativeError` parameter. 
The lower and upper bin bounds will be `-Infinity` and `+Infinity`, covering all real values.

This machine learning plugin represents a transformation stage that leverages the Apache Spark ML 
"Quantile Discretizer" to map continuous features of a certain input field onto binned categorical features.

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Data Configuration
**Input Field**: The name of the field in the input schema that contains the features.

**Output Field**: The name of the field in the output schema that contains the transformed features.

### Parameter Configuration
**Number of Buckets**: The number of buckets (quantiles, or categories) into which data points are grouped.
Must be greater than or equal to 2. Default is 2.

**Relative Error**: The relative target precision for the approximate quantile algorithm used to generate 
buckets. Must be in the range [0, 1]. Default is 0.001.
