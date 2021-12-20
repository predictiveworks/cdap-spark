
# TS ACF

## Description

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

## Model Configuration
**Model Name**: The unique name of the machine learning model.

**Model Stage***: The stage of the ML model. Supported values are 'experiment', 'staging', 'production'
and 'archived'. Default is 'experiment'.

### Data Configuration
**Time Field**: The name of the field in the input schema that contains the time value.

**Value Field**: The name of the field in the input schema that contains the value.

### Parameter Configuration
**Maximum Lag Order**: The maximum lag value. Use this parameter if the ACF is based on a range of lags. 
Default is 1.

**Discrete Lags**: The comma-separated sequence of lag value. Use this parameter if the ACF should be based on 
discrete values. This sequence is empty by default.

**Correlation Threshold**: The threshold used to determine the lag value with the highest correlation score. 
Default is 0.95.
