# Works ML

This project aims to implement the vision of **Visual ML** - Code-free orchestration of data pipelines (or workflow) to respond to machine learning use cases.

**Works ML** integrates [Apache Spark MLlib](https://spark.apache.org/) machine learning library with [Google CDAP](https://cdap.io) and offers approved ML features as plugins for CDAP data pipelines.

<img src="https://github.com/predictiveworks/cdap-spark/blob/master/works-ml/images/works-ml.png" width="800" alt="Works ML">

The following ML features are supported:

## Feature Engineering

### Conversion

* Binarizer
* Discrete Cosine
* Index-to-String
* Normalizer
* One Hot Encoder
* Quantile Discretizer
* String-to-Index 
* Vector Assembler
* Vector Indexer

### Scaling

* Max-Abs Scaler
* Min-Max Scaler 
* Standard Scaler 

### Selection

* Chi-Squared Selector  

## Text Processing

* Count Vectorizer
* Hashing TF
* Regex Tokenizer
* TF-IDF
* Word2Vec

## Machine Learning

### Classification

* Decision Tree
* Gradient-Boosted Trees
* Logistic Regression
* Multilayer Perceptron
* Naive Bayes
* Random Forest

### Clustering

* Bisecting K-Means
* Gaussian Mixture
* K-Means
* LDA

### Dimensionality Reduction

* PCA

### Recommendation

* ALS
* SAR

### Regression

* AFT Survival
* Decision Tree
* Generalized Linear Regression
* Gradient-Boosted Trees
* Isotonic Regression
* Linear Regression
* Random Forest

## Model Tracking

**Model Tracking:** Works ML uses Google CDAP *datasets* to support storing & retrieving of model components and associated metadata such as chosen parameters and model metrics and more. 
