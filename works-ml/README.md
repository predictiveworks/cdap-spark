# Works ML

This project aims to implement the vision of **Visual ML** - Code-free orchestration of data pipelines (or workflow) to respond to machine learning use cases.

**Works ML** integrates [Apache Spark MLlib](https://spark.apache.org/) machine learning library with [Google CDAP](https://cdap.io) and offers approved ML features as plugins for CDAP data pipelines.

<img src="https://github.com/predictiveworks/cdap-spark/blob/master/works-ml/images/works-ml.png" width="800" alt="Works ML">

The following ML features are supported:

## Feature Engineering

Conversion | Scaling | Selection
--- | --- | ---

Binarizer | Max-Abs Scaler | Chi-Squared Selector  
Discrete Cosine | Min-Max Scaler | 
Index-to-String | Standard Scaler | 
Normalizer | | 
One Hot Encoder | | 
Quantile Discretizer | | 
String-to-Index | | 
Vector Assembler | | 
Vector Indexer | | 

## Text Processing

### Count Vectorizer

### Hashing TF

### Regex Tokenizer

### TF-IDF

### Word2Vec

Word2Vec is an algorithm which takes sequences of words (representing documents or sentences) and trains a Word2Vec model. 
The model maps each word to a unique fixed-size vector, and transforms each document into a vector using the average of all words in the document.

This vector can then be used as features for prediction, document similarity calculations, etc.

## Machine Learning

### Classification

#### Decision Tree

#### Gradient-Boosted Trees

#### Logistic Regression

#### Multilayer Perceptron

#### Naive Bayes

#### Random Forest

### Clustering

#### Bisecting K-Means

Bisecting k-Means is a kind of hierarchical clustering using a divisive (or "top-down") approach: all observations start in one cluster, and splits are performed recursively as one moves down the hierarchy.

Bisecting K-Means can often be much faster than regular K-Means, but it will generally produce a different clustering.

#### Gaussian Mixture

A Gaussian Mixture Model represents a composite distribution whereby points are drawn from one of k Gaussian sub-distributions, each with its own probability. The [Apache Spark](https://spark.apache.org) implementation uses the expectation-maximization algorithm to induce the maximum-likelihood model given a set of samples.

#### K-Means

K-Means is one of the most commonly used clustering algorithms that clusters the data points into a predefined number of clusters. The [Apache Spark](https://spark.apache.org)  implementation includes a parallelized variant of the k-means++ method called kmeans||.

#### LDA

Latent Dirichlet Allocation (LDA) is a generative probabilistic model for collections of discrete data such as text corpora. LDA is a three-level hierarchical Bayesian model, in which each item of a collection is modeled as a finite mixture over an underlying set of topics. 

Each topic is, in turn, modeled as an infinite mixture over an underlying set of topic probabilities. In the context of
text modeling, the topic probabilities provide an explicit representation of a document. 

### Dimensionality Reduction

#### PCA

### Recommendation

### Regression

#### AFT Survival

#### Decision Tree

#### Generalized Linear Regression

#### Gradient-Boosted Trees

#### Isotonic Regression

#### Linear Regression

#### Random Forest

## Model Tracking

**Model Tracking:** Works ML uses Google CDAP *datasets* to support storing & retrieving of model components and associated metadata such as chosen parameters and model metrics and more. 
