


<p align="center">
<img src="https://github.com/predictiveworks/cdap-spark/blob/master/images/works-ts.svg" width="360" alt="Works TS"> 
</p>

#
This project aims to implement the vision of **Visual TS** - Code-free orchestration of data pipelines (or workflow) to respond to analysis use cases for time series data.


## Feature Engineering

### Aggregate

### Interpolate

Working with time series data often suffers from missing entries. **Interpolate** is a CDAP computation plugin
that addresses this issue for Apache Spark DataFrames.

### Lagging

### Resample

## Auto Correlation Function (ACF)

A frequent requirement for many time series analysis methods is that the data need to be stationary (i..e mean, variance and auto correlation structure do not change of time). For practical purposes, stationarity is usually determine from linear auto correlation functions (ACF).

**Works TS** ACF plugin calculates the auto correlation coefficients as Pearson coefficients between the value of the time series x(t) as time t and its past values at times t-1, ..., t-N. Correlation coefficients between -0.5 and 0.5 would be considered as low correlation, while those outside this range indicate high(er) correlation.

**Works TS** ACF plugin computes the values of the auto correlation function aand also determines the time lag with the highest (positive) correlation coefficient above a certain threshold (to skip irrelevant maxima). This time lag can then be intepreted as the seasonality pattern or period of the time series under consideration.

## Random Forest Regression

## Seasonal and Trend decomposition using Loess (STL)

### STL

### AutoSTL

## Time Models

Supported criteria for parameter tuning:

| Criterion | Description |
| --- | --- |
| AIC | Akaike information criterion |
| AICC | Akaike information criterion with corrections for finite sample sizes |
| BIC | Bayesian information criterion |


### ARIMA 

A popular and widely used statistical method for time series forecasting is the ARIMA model. ARIMA is short for *AutoRegressive Integrated Moving Average*. 
It is a class of model that captures a suite of different standard temporal structures in time series data.

An ARIMA model is a class of statistical models for analyzing and forecasting time series data. It explicitly caters to a suite of standard structures in 
time series data, and as such provides a simple yet powerful method for making skillful time series forecasts.
ARIMA is a generalization of the simpler AutoRegressive Moving Average (ARMA) model and adds the notion of integration.

The key aspects of the model are:

**AR**: *Autoregression*. A model that uses the dependent relationship between an observation and some number of lagged observations.

**I**: *Integrated*. The use of differencing of raw observations (subtracting an observation from an observation at the previous time step) in order 
to make the time series stationary (see STL decomposition for an alternative approach).

**MA**: *Moving Average*. A model that uses the dependency between an observation and a residual error from a moving average model applied to lagged observations.

Each of these components are explicitly specified in the ARIMA model as parameters.

### ARMA

Autoregressive moving average (ARMA) regression: The ARMA model plays an important role in understanding & predicting future values in the time series. 
The model combines both AR model and MA model. 

The AR part fits the values of the series on its own lagged values and the MA part linearly models the error term with the past errors.

### AR YuleWalker

### AR

### Auto-AR

### Auto-ARIMA

### Auto-ARMA

The Auto-ARMA model finds best parameters for an ARMA model (p: order of the autoregressive part, q: order of the moving average part) according to the different information criteria:

* AIC
* AICC
* BIC

### Auto-MA

### Auto Regression

### DiffAutoRegression

### Moving Average

## Demand Prediction

### Regression

> Knowing what lies ahead in the future makes life much easier. This is true for life events, prices of clothes, the demand for electrical power in an entire city and may other predictive questions.

Demand prediction is a complex time series analysis problem, but in many use cases it can be satisfactorily transformed into a regression problem.

For these use cases **Works TS** supports model building of *Random Forest* regression models. 


*to be continued*
