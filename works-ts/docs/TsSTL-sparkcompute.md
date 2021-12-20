
# TS STL

## Description

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Data Configuration
**Time Field**: The name of the field in the input schema that contains the time value.

**Value Field**: The name of the field in the input schema that contains the value.

**Group Field**: The name of the field in the input schema that contains the group value required by
decomposition algorithm.

### Parameter Configuration
**Outer Iterations**: The positive number of cycles through the outer loop. More cycles here reduce the 
affect of outliers. For most situations this can be quite small. Default is 1.

**Inner Iterations**: The positive number of cycles through the inner loop. Number of cycles should be 
large enough to reach convergence,  which is typically only two or three. When multiple outer cycles, the 
number of inner cycles can be smaller as they do not necessarily help get overall convergence. 
Default value is 2.

**Periodicity**: The periodicity of the seasonality; should be equal to lag of the auto correlation 
function with the highest (positive) correlation.

**Seasonal Smoother**: The length of the seasonal LOESS smoother.

**Trend Smoother**: The length of the trend LOESS smoother.

**Level Smoother**: The length of the level LOESS smoother.
