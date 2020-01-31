# CDAP Spark

**CDAP Spark** is an all-in-one library for unified **Visual Analytics**. CDAP Spark is sitting on the shoulders of [Apache Spark](https://spark.apache.org), which now is the big data platform of choice for enterprises.

CDAP Spark covers all flavors of modern data analytics from deep learning, machine learning to busines rule and query analysis up to comprehensive text & time series processing.

It *externalizes* modern data analytics in form of plugins for [Google CDAP](https://cdap.io) data pipelines, and boosts the work of data analysts and scientists 
to build data driven applications without coding.

We decided to select [**Google's CDAP**](https://cdap.io) as this unified environment was designed to cover all aspects of corporate data processing, from data integration & ingestion to SQL & business rules up to machine learning & deep learning.

>CDAP Spark offers than 150 analytics plugins for [CDAP](https://cdap.io) based pipelines and provides the world's largest collection of visual analytics components.

CDAP Spark is part and main building block of [PredictiveWorks](https://predictiveworks.eu).

![alt CDAP Spark](https://github.com/predictiveworks/cdap-spark/blob/master/images/cdap-spark.png)

## What is unified Visual Analytics?

Nowadays many excellent open source big data analytics & computing libraries exist, but each with a certain focus or lens on the data universe. Answering predictive business questions most often requires to operate many of them, with human data and software experts in the loop to stick pieces individually together.

An integrated data analytics platform that seamlessly covers all flavors of modern data analytics seems to be a utopia for data-driven enterprises.

>This project aims to prove that nowadays all technical building blocks exist to build a unified corporate data analytics environment, *based on approved libraries*, and, with a visual interface to build data pipelines without any coding experience.

The image below illustrates a 3-phase approach how existing distributed analytics libraries can be transformed into pluggable components to support flexible pipeline orchestration. 

<img src="https://github.com/predictiveworks/cdap-spark/blob/master/images/steps-to-visual-analytics.png" width="800" alt="Visual Analytics">

**CDAP Spark** is an implementation of this approach and covers the modules for comprehensive unified analytics:

## Modules

<img src="https://github.com/predictiveworks/cdap-spark/blob/master/images/works-dl.svg" width="800" alt="Works DL"> | <img src="https://github.com/predictiveworks/cdap-spark/blob/master/images/works-ml.svg" width="800" alt="Works ML"> | <img src="https://github.com/predictiveworks/cdap-spark/blob/master/images/works-ts.svg" width="800" alt="Works TS"> 
 :---: | :---: | :---: |
 **Deep Learning** | **Machine Learning** | **Time Series**

<img src="https://github.com/predictiveworks/cdap-spark/blob/master/images/works-rules.svg" width="800" alt="Works Rules"> | <img src="https://github.com/predictiveworks/cdap-spark/blob/master/images/works-sql.svg" width="800" alt="Works SQL"> | <img src="https://github.com/predictiveworks/cdap-spark/blob/master/images/works-text.svg" width="800" alt="Works Text"> 
 :---: | :---: | :---: |
 **Drools Rules** | **Spark SQL** | **Text Analysis**

> Externalization is an appropriate means to make advanced analytics reusable, transparent and notably secures the knowledge how enterprise data are transformed into insights, foresights and knowledge.

## Model Management

Corporate adoption of machine learning or deep learning often runs into the same problem. A variety of existing (open source) solutions & engines enable data scientists to develop data models very fast. However, integrating trained models into business processes is a completely different story:

* Different infrastructures and technologies in production environments either demand to reimplement important steps of model
building, or completely prevent the usage of trained models in application processes.

* Great machine learning or deep learning solutions with a wide range of sophisticated algorithms run the risk to produce small or no effect for corporate data processing.

This project focuses on the immediate corporate usability of machine learning & deep learning results and supports model building & usage within the **same unified** technical environment.

<img src="https://github.com/predictiveworks/cdap-spark/blob/master/images/model-management.png" width="800" alt="Model Management">

## Why not use Seahorse Visual Spark?

[Seahorse](https://seahorse.deepsense.ai) is built by [deepsense.ai](https://deepsense.ai) with the aim to create [Apache Spark](https://spark.apache.org) applications in a fast, simple and interactive way - based on a visual editor.

CDAP Spark is also based on Apache Spark as its foundation for distributed in-memory processing. But it is not restricted to Apache Spark's machine learning channel. CDAP Spark complements this channel with deep learning, timeseries and als business rule support.

And, of course, CDAP Spark is just one building block of [PredictiveWorks.](https:predictiveworks.eu)  

## Overview

**CDAP Spark** is designed to offer all instruments of corporate data processing and contains the following modules:

### Works Core

**Works Core** provides common functionality that is used by other modules of this project.

### Works DL

**Works DL** externalizes deep learning algorithms (adapted from Intel's BigDL project) as plugins for Google CDAP data pipelines. Pipelines can be built without coding by either leveraging CDAP's visual pipeline editor or **Predictive Works.** template studio.

### Works ML

**Works ML** externalizes Apache Spark machine learning algorithms as plugins for Google CDAP data pipelines.These pipelines can also be built visually without any coding expertise.

### Works TS

**Works TS** complements Apache Spark with time series algorithms and also externalizes them as plugins for Google CDAP data pipelines.

### Works Rules

**Works Rules** externalizes [Drools' Rule Engine](https://www.drools.org) as plugin for CDAP data pipelines. Drools compliant rules can be applied by an easy-to-use interface without the need to write code in any programming language.

Drools business rules can be used with batch and stream pipelines, and, in the case of the latter ones, support complex event processing in real-time applications.

### Works SQL

**Works SQL** supports the application of Apache Spark compliant SQL queries for CDAP batch and stream pipelines. SQL statements can be specified by leveraging an easy-to-use interface and offer aggregation, grouping & filtering support e.g. for real-time applications. 

![alt Works SQL](https://github.com/predictiveworks/cdap-spark/blob/master/images/works-sql.png)

### Works Text

**Works Text** integrates [John Snow Lab's](https://nlp.johnsnowlabs.com/) excellent **Spark NLP** library with [Google CDAP](https://cdap.io) and offers approved NLP features as plugins for CDAP data pipelines.

