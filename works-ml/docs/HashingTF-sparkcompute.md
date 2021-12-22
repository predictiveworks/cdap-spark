
# Hashing TF

## Description
HashingTF is a Transformer which takes sets of terms and converts those sets into 
fixed-length feature vectors. In text processing, a 'set of terms' might be a bag 
of words. HashingTF utilizes the hashing trick. 

A raw feature is mapped into an index (term) by applying a hash function. The hash 
function used here is MurmurHash 3. Then term frequencies are calculated based on 
the mapped indices. 

This approach avoids the need to compute a global term-to-index map, which can be 
expensive for a large corpus, but it suffers from potential hash collisions, where 
different raw features may become the same term after hashing. 

To reduce the chance of collision, we can increase the target feature dimension, i.e. 
the number of buckets of the hash table. Since a simple modulo is used to transform 
the hash function to a column index, it is advisable to use a power of two as the 
feature dimension, otherwise the features will not be mapped evenly to the columns. 

The default feature dimension is 218=262,144.

This machine learning plugin represents a transformation stage that leverages the Apache Spark ML "Hashing TF" 
algorithm and maps a sequence of terms to their term frequencies using the hashing trick. Currently, the Austin 
Appleby's MurmurHash 3 algorithm is used.

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Data Configuration
**Input Field**: The name of the field in the input schema that contains the features.

**Output Field**: The name of the field in the output schema that contains the transformed features.

### Parameter Configuration
**Number of Features**: The nonnegative number of features to transform a sequence of terms into.
