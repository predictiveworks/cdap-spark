
# Scaler Builder

## Description
This machine learning plugin represents a building stage for an Apache Spark ML feature Scaler model. 
Supported models are 'Min-Max', 'Max-Abs' and 'Standard' Scaler.

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Model Configuration
**Model Name**: The unique name of the machine learning model.

**Model Stage***: The stage of the ML model. Supported values are 'experiment', 'staging', 'production'
and 'archived'. Default is 'experiment'.

**Model Type**: The type of the scaler model. Supported values are 'maxabs', 'minmax' and 'standard'.
Default is 'standard'.

### Data Configuration
**Input Field**: The name of the field in the input schema that contains the features.

### Parameter Configuration
**Lower Bound**: The lower bound of the feature range after transformation. This parameter is restricted 
to the model type 'minmax'. Default is 0.0.

**Upper Bound**: The upper bound of the feature range after transformation. This parameter is restricted 
to the model type 'minmax'. Default is 1.0.

**With Mean**: Indicator to determine whether to center the data with mean before scaling. This parameter 
applies to the model type 'standard'. Default is 'false'.

**With Std**: Indicator to determine whether to scale the data to unit standard deviation. This parameter 
applies to the model type 'standard'. Default is 'true'.
