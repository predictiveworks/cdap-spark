
# TS Yule Walker Builder

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

**Time Split**: The split of the dataset into train & test data, e.g. 80:20. Note, this is a split time
and is computed from the total time span (min, max) of the time series. Default is 70:30.

### Parameter Configuration
**Lag Order**: The positive number of lag observations included in the time series model (also called 
the lag order).