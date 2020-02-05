# Business Predictions Maturity Model (BPMM)

## Objective
The *BPMM* is a practical guide to **Unified Visual Analytics** for enterprises at any scale with actionable recommendations based on (existing) open-source solutions. Its focus is on the corporate adoption of **predictive analytics**.

## Visual Analytics

Visual Analytics is a technical concept that aims to offer the spectrum of modern analytics, 
* business rules, 
* deep learning ,
* machine learning,
* natural language processing, 
* query analysis, and, 
* time series analysis 

as code-free, visual components that can be used to gain hindsight, insights and foresights for users without programming skills.

## Why BPMM?
The landscape of modern big data and scalable analytics solutions is complex and often opaque for many enterprises. The approch to give value to the continuously increasing amount of data typically ends in a plethora of different technologies and solutions with little interoperability.

We want to share this business maturity model to help companies gain sustainable value from their data with a strong focus on 
* unification, 
* standardization, and, 
* visual analytics. 

The *BPMM* takes a 360° view on corporate analytics: predictive applications always come with a business context that defines the root cause of a certain data solution. Solving a certain business problem & answering predictive questions often requires a set of data solutions that implements the different aspects or tasks originating from the business problem.

All this is tremendous treasure of business knowledge *what enterprise do with their data* and has to be persisted and made accessible with ease to respond to future business problems.

## Implementaion

The BPMM builds the foundation for [**PredictiveWorks.**](https://predictiveworks.eu)

## Maturity Levels

The *BPMM* is the result of a long standing experience defines 5 maturity levels how companies can approach predictive analytics with ease.

<img src="https://github.com/predictiveworks/cdap-spark/blob/master/images/maturity-levels.png" width="800" alt="Maturity Levels">

### Level 1

At this level, corporate adoption of predictive analytics is guided by management terms such as "minimal viable product", "pilot project", "quick wins" and more. Such a situation usually indicates that no robust data (analytics) strategy exists. 

> Individual business sections launch hardly reusable software and data science projects which often *prove* fast that "something works".

The BPMM is guided by a completely different approach: The competitive advantage of predictive analytics is obvious. Therefore, the BPMM aims to support the fast adoption of a **production-ready** data analytics platform which covers all aspects of modern data processing, from data integration to query & rule based analysis up to machine and deep learning.

To put it on a more conceptual level (see Gartner's Analytics Continuum): The BPMM aims to cover following questions with a unified approach:

* What happended?
* Why did it happened?
* What will happen?
* How can we make it happen?

At the heart of each process that transforms data into valuable business insights and foresights is a single conceptual kernel: a data workflow or pipeline. This concept is supported by almost all existing data solutions & engines. 

Solutions for complex event processing of real-time events, machine learning, deep learning and more all support the (programmatic) implementation of data pipelines, **but individually**.  

Companies located at this maturity level (and who have defined a data analytics strategy already) are challenged to implement data pipelines on top of these purpose-built solutions & engines to gain a production-ready 360° business view on their data. 

Symptoms, such as "trained machine learning models remain undeployable for prediction workflows", indicate that the implementation of these comprehensive data pipelines still fails. 

### Level 2

Companies at this maturity level understand that the adoption of (business-ready) predictive analytics does not start with the implementation of a certain prototype for demand prediction, e.g. in an Internet-of-Things context. 

They focus on the corporate adoption of a unified data pipeline solution that standardizes all aspects of data processing as pluggable and reusable stages. At this level, data integration use cases are mapped onto a set of pluggable data connectors, e.g. for SaaS applications. 

Complex event event processing is supported by pluggable business rule & query engines. And even model building and deployment is mapped onto a reusable pipeline plugins.

The BPMM recommends to leverage the comprehensive pipeline solution CDAP of former Cask Inc. Recently, this open source solution has been acquired by Google and put at the heart of their Cloud Data Fusion service. 

CDAP is also at the heart of [PredictiveWorks.](https://predictiveworks.eu)

Companies who are willing to follow the CDAP approach immediately benefit from a variety of pre-built data connectors that boost data integration use cases without the need to write code. In addition, companies gain a standardized technical environment to build their own reusable pipeline plugins. 

### Level 3

Companies at this maturity level benefit from [PredictiveWorks.](https://predictiveworks.eu) and build their data pipelines for complex event processing, machine learning & deep learning with a wide variety of pre-built pipeline plugins that go far beyond CDAP's data integration plugins.

[**CDAP-Spark**](https://github.com/predictiveworks/cdap-spark/blob/master/README.md) is one of the building blocks of PredictiveWorks. and offers [Apache Spark](https://spark.apache.org) based data analytics in form of CDAP compliant pipeline stages. This projects offers an all-in-one solution for

* Deep Learning,
* Machine Learning,
* Time Series Analysis,
* Drools Business Rules,
* SQL Queries, and
* Text Analysis.

Companies can use these pre-built analytics plugins to visually orchestrate their data workflows based on either CDAP's visual pipeline studio or, as an alternative, PredictiveWorks. template studio (editor).

At maturity level 3, companies understand that data pipelines specify *what they do with their data* and manifest a huge treasure of business knowledge.
This persisted knowledge base can be used to ease onboarding of employees or put as a configurable templates to respond to similar uses cases in the future.  

### Level 4: Pipeline (Knowledge) Management

Data pipelines are at the heart of each data-driven company. Each business question that can be answered by analyzing data and each data-driven business decision generates new or updates existing data pipelines.

>In most cases the amount of business data pipelines is going to increase (often very) fast and requires an appropriate **pipeline management**.

As data pipelines define **templated knowledge** on a *technical* level, there is an increasing demand to relate them to the orginating business and tasks that specify their context and root cause.

At maturity level 4 the concept of **business (knowledge) templates** is introduced to that cover the complete range from specifying a business case, its decomposition into business tasks and the implementation of tasks as (technical) data pipelines.

Data pipelines **and** its business context are persisted and made available through a **Template Market** approach, accompanied by a recommendation system.

Companies at this level benefit from a knowledge base that makes building predictive data applications as easy as purchasing a certain product.

[PredictiveWorks.](https://predictiveworks.eu) implements a mature pipeline knowledge management as one its major building blocks.

### Level 5

Companies at the previous level (still) work on their own to answer predictive business questions. PredictiveWorks. offers a tremendous simplification and reduction of one of the most important KPIs: **Time-to-Value**.

At level 5, companies understand that sharing their business templates with others and taking advantage of shared templates boosts their business a second time.

[PredictiveWorks.](https://predictiveworks.eu) is also designed as a template sharing solution. For companies, who want to experience the advantages of this top materity model, PredictiveWorks. offers pre-built business prediction templates for the following use cases:

* Cyber Defense,
* Internet-of-Things,
* AI Marketing and
* E-Commerce.

These prediction templates are offered as seeds to initiate a cross-business sharing of prediction templates.
