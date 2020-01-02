# CDAP-Spark
A wrapper for Apache Spark to make machine &amp; deep learning available as Google CDAP plugins

Corporate adoption of machine learning or deep learning often runs into the same problem:

* A variety of (open source) solutions & engines enable data scientists to develop data models very fast. However, integrating 
trained models into business processes is a completely different story:

* Different infrastructures and technologies in production environments either demand to reimplement important steps of model
building, or completely prevent the usage of trained models in application processes.

* Great machine learning or deep learning solutions with a wide range of sophisticated algorithms run the risk to produce small or no effect for corporate data processing.

This project focuses on the immediate corporate usability of machine learning & deep learning results and supports model building & usage within the **same unified** technical environment.

We decided to select [**Google's CDAP**](https://cdap.io). This unified environment has been designed to cover all aspects of corporate data processing, from data integration & ingestion to SQL & business rules up to machine learning & deep learning.

This project externalizes [**Apache Spark**](https://spark.apache.org) machine learning as CDAP data pipeline stages, adds missing time series analytics to Apache Spark and also makes [**Intel's BigDL**](https://bigdl-project.github.io/) library accessible as CDAP pipeline stages.   

CDAP Spark is part of [Predictive Works](https://predictiveworks.eu). The picture below shows its main building blocks of and their relations to this project. 

![alt CDAP Spark](https://github.com/predictiveworks/cdap-spark/blob/master/images/cdap-spark.svg)
