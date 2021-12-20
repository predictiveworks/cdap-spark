
# TS ARIMA Builder

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

**Differencing Degree**: The positive number of times that the raw observations are differenced (also called 
the degree of differencing).

**Moving Average Order**: A positive number that specifies the size of the moving average window (also called 
the order of moving average).

**ElasticNet Mixing**: The ElasticNet mixing parameter. For value = 0.0, the penalty is an L2 penalty. 
For value = 1.0, it is an L1 penalty. For 0.0 < value < 1.0, the penalty is a combination of L1 and L2. 
Default is 0.0.

**Regularization Parameter**: The nonnegative regularization parameter. Default is 0.0.

**Standardization**: The indicator to determine whether to standardize the training features before fitting 
the model. Default is 'true'.

**With Intercept**: The indicator to determine whether to fit an intercept value.

**Remove Mean**: The indicator to determine whether to remove the mean value from the value from the value 
of the time series before training model. Default is 'false'.
