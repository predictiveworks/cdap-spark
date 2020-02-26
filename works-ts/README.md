# Works TS

## Overview
PredictiveWorks. time series support has a strong focus
on forecasting and prediction. Data are often sparse in time, non-stationary,
carry seasonality pattern and trends.         

A frequent requirement for time series techniques is that the data be stationary. This argument
holds for the time series models supported here as well.

PredictiveWorks. responds to this fact and offers, in
addition to model building, forecasting and prediction, many standardized plugins for
preprocessing, seasonality & trend decomposition, and time series engineering.

## Time Series Tasks

### Preprocessing
PredictiveWorks. supports a variety of data preparation
techniques to transform sparse time series data to a series of values uniformly sampled
in time. This includes: 
* Aggregation, 
* Auto Correlation
* Interpolation
* Resampling

### Decomposition
Time series data often carry seasonality pattern and trends and are non-stationary. Differencing
is a frequent technique to make time series data stationary and ready for model building.

Seasonality and trends, however, have its own value. PredictiveWorks.
supports decomposition of time series data into their seasonal, trend and remaining part.

This preserves and extracts meaningful information and also prepares data for model building.

### Engineering
Many general purpose ML models such as clustering, regression and others, start from feature
vectors and labels. To benefit from these ML models, time series data have to be transformed
into (labeled) feature vectors.

PredictiveWorks. time series engineering supports time series embedding into higher-dimensional 
feature spaces.

### Forecasting
PredictiveWorks. offers approved time series models as standardized
pipeline plugins to look several steps ahead in time and forecast the values of a dependent variable.

Plugins for model building and forecasting share the same technology and make sure that trained and
retrained time series models are immediately available for production pipelines.

The following time series models are supported: 
* ARIMA 
* Auto ARIMA
* ARMA
* Auto ARMA
* Auto Regression
* Differenced Auto Regression 
* Moving Average
* Auto Moving Average
* Yule Walker Regression

### Prediction
For many use cases, including demand prediction, forecasting what lies ahead in the future can be
satisfactorily and easily solved after transforming it into a classification or regression problem.

PredictiveWorks. supports time series engineering to seamlessly integrate
time series analytics with general purpose classification and regression.

In addition to this, Predictive>Works. offers support for Random Forest Regression
to predict the most probable value for the next point or interval in time.

## Integration

PredictiveWorks. integrates <b>Spark Time</b>, a time series analysis library that
has been open-sourced recently by Dr. Krusche & Partner. Its functional scope was externalized as standardized plugins
for Google CDAP pipelines and can be easily combined with any other plugin.

<img src="https://github.com/predictiveworks/cdap-spark/blob/master/works-ts/images/works-ts.svg" width="95%" alt="Works TS">
