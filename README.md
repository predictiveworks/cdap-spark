# CDAP Spark

**CDAP Spark** is an all-in-one library for unified **plug & play**. CDAP Spark is sitting on the shoulders of [Apache Spark](https://spark.apache.org), which now is the big data platform of choice for enterprises.

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

## Background

Interested into more detailed information? [Read here](https://predictiveworks.github.io)