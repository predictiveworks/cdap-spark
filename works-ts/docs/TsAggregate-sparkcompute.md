
# TS Aggregate

## Description

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Data Configuration
**Time Field**: The name of the field in the input schema that contains the time value.

**Value Field**: The name of the field in the input schema that contains the value.

**Group Field**: The name of the field in the input schema that specifies data groups.

### Parameter Configuration
**Aggregation Method**: The name of the aggregation method. Supported values are 'avg', 'count', 
'mean' and 'sum'. Default is 'avg'.

**Window Duration**: The time window used to aggregate intermediate values. Default is '10 minutes'.
