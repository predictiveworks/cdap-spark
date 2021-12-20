
# Discrete Cosine

## Description
The Discrete Cosine Transform transforms a length N real-valued sequence in the time domain into another 
length N real-valued sequence in the frequency domain.

A DCT class provides this functionality, implementing the DCT-II and scaling the result by 1/2‾√ such that 
the representing matrix for the transform is unitary.
 
No shift is applied to the transformed sequence, e.g. the 0th element of the transformed sequence is the 
0th DCT coefficient and not the N/2th.

This machine learning plugin represents the transformation stage that leverages the Apache Spark ML "Discrete Cosine 
Transform" to map a feature vector in the time domain into a feature vector in the frequency domain.

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Data Configuration
**Input Field**: The name of the field in the input schema that contains the features.

**Output Field**: The name of the field in the output schema that contains the transformed features.

**Inverse**: An indicator to determine whether to perform the inverse DCT (true) or forward DCT (false). 
Default is 'false'.
