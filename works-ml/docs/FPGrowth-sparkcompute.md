
# FPGrowth Miner

## Description
This machine learning plugin represents a transformation stage that leverages the Apache Spark ML "FPGrowth algorithm" 
to detect frequent patterns.

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Data Configuration
**Items Field**: The name of the field in the into schema that contains the items.

### Parameter Configuration
**Minimal Support**: Minimal support level of the frequent pattern. Value must be in range [0.0, 1.0]. 
Any pattern that appears more than (minSupport * data size) times will be output in the frequent item sets. 
Default is 0.3.

**Minimal Confidence**: Minimal confidence for generating Association Rule. minConfidence will not affect 
the mining for frequent item sets, but will affect the association rules generation. Default is 0.8.
