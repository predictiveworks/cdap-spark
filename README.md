# CDAP Spark

**CDAP Spark** is an all-in-one library for unified **Visual Analytics**. CDAP Spark is sitting on the shoulders of [Apache Spark](https://spark.apache.org), which now is the big data platform of choice for enterprises.

CDAP Spark covers all flavors of modern data analytics from deep learning, machine learning to busines rule and query analysis up to comprehensive text & time series processing.

<img src="https://github.com/predictiveworks/cdap-spark/blob/master/images/works-dl.svg" width="800" alt="Works DL"> | <img src="https://github.com/predictiveworks/cdap-spark/blob/master/images/works-ml.svg" width="800" alt="Works ML"> | <img src="https://github.com/predictiveworks/cdap-spark/blob/master/images/works-ts.svg" width="800" alt="Works TS"> 
 :---: | :---: | :---: |

<img src="https://github.com/predictiveworks/cdap-spark/blob/master/images/works-rules.svg" width="800" alt="Works Rules"> | <img src="https://github.com/predictiveworks/cdap-spark/blob/master/images/works-sql.svg" width="800" alt="Works SQL"> | <img src="https://github.com/predictiveworks/cdap-spark/blob/master/images/works-text.svg" width="800" alt="Works Text"> 
 :---: | :---: | :---: |

CDAP Spark *externalizes* modern data analytics in form of plugins for [Google CDAP](https://cdap.io) data pipelines, and boosts the work of data analysts and scientists 
to build data driven applications without coding.

> Externalization is an appropriate means to make advanced analytics reusable, transparent and notably secures the knowledge how enterprise data are transformed into insights, foresights and knowledge.

We decided to select [**Google's CDAP**](https://cdap.io) as this unified environment was designed to cover all aspects of corporate data processing, from data integration & ingestion to SQL & business rules up to machine learning & deep learning.

>CDAP Spark offers more than **150 analytics plugins** for [CDAP](https://cdap.io) based pipelines and provides the world's largest collection of visual analytics components.

## What is unified Visual Analytics?

Nowadays many excellent open source big data analytics & computing libraries exist, but each with a certain focus or lens on the data universe. Answering predictive business questions most often requires to operate many of them, with human data and software experts in the loop to stick pieces individually together.

An integrated data analytics platform that seamlessly covers all flavors of modern data analytics seems to be a utopia for data-driven enterprises.

>This project aims to prove that nowadays all technical building blocks exist to build a unified corporate data analytics environment, *based on approved libraries*, and, with a visual interface to build data pipelines without any coding experience.

The image below illustrates a 3-phase approach how existing distributed analytics libraries can be transformed into pluggable components to support flexible pipeline orchestration. 

<img src="https://github.com/predictiveworks/cdap-spark/blob/master/images/steps-to-visual-analytics.png" width="800" alt="Visual Analytics">

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

## PredictiveWorks.

**CDAP Spark** is part of a project family and an important building block of [PredictiveWorks.](https://predictiveworks.eu) The latter is an initiative to overcome all the barriers & stumbling blocks of predictive analytics and make it actionable for everyone.

>The game is not to bring more and more sophisticated algorithms onto the market and fight a fight which neural network is better than others. While companies are still trouble to integrate their cloud data silos and preprare for analytics. Existing algorithms are good enough to answer the overwhelming part of predictive business questions. The real game is to make them actionable for everyone.

![alt CDAP Spark](https://github.com/predictiveworks/cdap-spark/blob/master/images/predictiveworks.png)

## Overview

Visual Analytics is supported by the following modules:

| Module | Description |
| --- | --- |
| DL | Externalizes deep learning algorithms (adapted from Intel's [Analytics Zoo](https://github.com/intel-analytics/analytics-zoo)) as plugins for Google CDAP data pipelines. |
| ML | Externalizes Apache [Spark ML](spark.apache.org) machine learning algorithms as Google CDAP data pipelines. |
| TS | Completes Apache Spark with proven time series algorithms and also externalizes them as plugins for Google CDAP data pipelines. |
| Rules | Externalizes [Drools' Rule Engine](https://www.drools.org) as plugin for CDAP data pipelines.
| SQL | Supports the application of Apache Spark compliant SQL queries for CDAP batch and stream pipelines. |
| Text | Integrates [John Snow Lab's](https://nlp.johnsnowlabs.com/) excellent **Spark NLP** library with [Google CDAP](https://cdap.io) and offers approved NLP features as plugins for CDAP data pipelines. |

