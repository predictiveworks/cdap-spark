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


## Seasonal and Trend decomposition using Loess (STL)

## Demand Prediction

### Regression

> Knowing what lies ahead in the future makes life much easier. This is true for life events, prices of clothes, the demand for electrical power in an entire city and may other predictive questions.

Demand prediction is a complex time series analysis problem, but in many use cases it can be satisfactorily transformed into a regression problem.

For these use cases **Works TS** supports model building of *Random Forest* regression models. 


*to be continued*
