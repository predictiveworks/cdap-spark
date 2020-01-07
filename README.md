# CDAP-Spark
**CDAP Spark** is an all-in-one library that *externalizes* Apache Spark based machine learning, deep learning and complex event processing in form of plugins for Google CDAP data pipelines. It boosts the work of data analysts and scientists to build data driven applications without coding.

![alt Works DL](https://github.com/predictiveworks/cdap-spark/blob/master/images/works-dl.svg) | ![alt Works ML](https://github.com/predictiveworks/cdap-spark/blob/master/images/works-ml.svg) | ![alt Works DL](https://github.com/predictiveworks/cdap-spark/blob/master/images/works-ts.svg) 
 :---: | :---: | :---: |
 **Deep Learning** | **Machine Learning** | **Time Series**
 BigDL Plugins | Apache Spark ML Plugins | Time Series Plugins


> Externalization is an appropriate means to make advanced analytics reusable, transparent and notably secures the knowledge how enterprise data are transformed into insights, foresights and knowledge.

## Why CDAP Spark?

Corporate adoption of machine learning or deep learning often runs into the same problem. A variety of existing (open source) solutions & engines enable data scientists to develop data models very fast. However, integrating trained models into business processes is a completely different story:

* Different infrastructures and technologies in production environments either demand to reimplement important steps of model
building, or completely prevent the usage of trained models in application processes.

* Great machine learning or deep learning solutions with a wide range of sophisticated algorithms run the risk to produce small or no effect for corporate data processing.

This project focuses on the immediate corporate usability of machine learning & deep learning results and supports model building & usage within the **same unified** technical environment.

We decided to select [**Google's CDAP**](https://cdap.io). This unified environment has been designed to cover all aspects of corporate data processing, from data integration & ingestion to SQL & business rules up to machine learning & deep learning.

This project externalizes [**Apache Spark**](https://spark.apache.org) machine learning as CDAP data pipeline stages, adds missing time series analytics to Apache Spark and also makes [**Intel's BigDL**](https://bigdl-project.github.io/) library accessible as CDAP pipeline stages.   

CDAP Spark is part of [Predictive Works](https://predictiveworks.eu). The picture below shows its main building blocks of and their relations to this project. 

![alt CDAP Spark](https://github.com/predictiveworks/cdap-spark/blob/master/images/cdap-spark.png)

## Overview

CDAP-Spark is designed to offer all instruments of corporate data processing and contains the following modules:

### Works Core

**Works Core** provides common functionality that is used by other modules of this project.

### Works DL

**Works DL** externalizes deep learning algorithms (adapted from Intel's BigDL project) as plugins for Google CDAP data pipelines. Pipelines can be built without coding by either leveraging CDAP's visual pipeline editor or **Predictive Works.** template studio.

### Works ML

**Works ML** externalizes Apache Spark machine learning algorithms as plugins for Google CDAP data pipelines.These pipelines can also be built visually without any coding expertise.

### Works Rules

**Works Rules** externalizes [Drools' Rule Engine](https://www.drools.org) as plugin for CDAP data pipelines. Drools compliant rules can be applied by an easy-to-use interface without the need to write code in any programming language.

Drools business rules can be used with batch and stream pipelines, and, in the case of the latter ones, support complex event processing in real-time applications.

### Works SQL

**Works SQL** supports the application of Apache Spark compliant SQL queries for CDAP batch and stream pipelines. SQL statements can be specified by leveraging an easy-to-use interface and offer aggregation, grouping & filtering support e.g. for real-time applications. 

![alt Works SQL](https://github.com/predictiveworks/cdap-spark/blob/master/images/works-sql.png)


### Works TS

**Works TS** complements Apache Spark with time series algorithms and also externalizes them as plugins for Google CDAP data pipelines.
