# Works ML

## Overview
PredictiveWorks. externalizes <a href="https://spark.apache.org">Apache Spark ML</a> machine learning
as standardized plugins, with the ability of seamless combination with any other plugin.

Feature engineering, classification, regression and more can be used via point-and-click selection
as pipeline components. The focus is on Apache Spark ML v2.1.3 to be compliant with Google CDAP's pipeline
technology.

The following machine learning tasks are supported by Works ML plugins:

## ML Tasks

### Classification
The classification scope of Apache Spark ML is externalized as standardized model building and
prediction plugins that share the same technology.

Trained and retrained classification models are immediately available for production pipelines.
Supported models & predictions:
* Decision Tree
* Gradient-Boosted Tree
* Logistic Regression
* Multi-Layer Perceptron
* Naive Bayes
* Random Forest

### Clustering
The clustering scope of Apache Spark ML is externalized as standardized model building and
prediction plugins that share the same technology.

Trained and retrained clustering models are immediately available for production pipelines.
Supported models & predictions:
* Bisecting K-Means
* Gaussian Mixture
* K-Means
* Latent Dirichlet Allocation

### Data Mining
The data mining scope of Apache Spark ML is externalized as standardized plugin
components. Supported mining tasks:
* Frequent Pattern (FP-Growth)

### Feature Engineering
The feature engineering scope of Apache Spark ML is externalized as standardized
pipeline plugins that can be seamlessly combined with model building and prediction
plugins to cover all facets of a machine learning process.

PredictiveWorks. complements Apache Spark ML with
a pipeline plugin for <b>SMOTE Sampling</b> to master imbalanced training sets for
ML classification tasks. Supported feature engineering tasks:
* Binarizer
* Bucketed LSH
* Bucketizer
* Chi-Squared Selector
* Count Vectorizer
* Discrete Cosine Transformation (DCT)
* Hashing TF
* Index to String
* Min-Hash LSH
* N-Gram Tokenizer
* Normalizer
* One-Hot Encoder
* Principal Component Analysis (PCA)
* Quantile Discretizer
* Scaling
* SMOTE Sampling
* String to Index
* TF-IDF
* Tokenizer
* Vector Assembler
* Vector Indexer
* Word-to-Vec Embeddings

### Recommendation
The recommendation scope of Apache Spark ML is externalized as standardized
model building and prediction plugins that share the same technology.

Trained and retrained recommendation models are immediately available for production pipelines.
Supported models & predictions:
* Collaborative Filtering (ALS)
* Smart Adaptive Recommendations (SAR)

### Regression
The regression scope of Apache Spark ML is externalized as standardized model building and
prediction plugins that share the same technology.

Trained and retrained regression models are immediately available for production pipelines.
Supported models & predictions:
* Decision Tree
* Gradient-Boosted Tree
* Generalized Linear Regression
* Isotonic Regression
* Linear Regression
* Random Forest
* Survival (AFT)

### SMOTE Sampling
In machine learning, *imbalanced* datasets are no surprise. If the datasets intended for classification problems
or other problems related to discrete predictive analytics have an unequal number of samples for different classes,
then those datasets are said to be imbalanced.

Classes having comparatively fewer instances than others are said to be a minority with respect to the classes having
a comparatively larger number of the samples (majority). Training ML models with imbalanced datasets often causes the
models to develop a certain bias towards the majority classes.

The SMOTE algorithm, short for Synthetic Minority Over-sampling Technique, addresses this issue imbalanced classes.
It is based on nearest neighbors (determined by the Euclidean distance of data points in the feature space).

The feature values of nearest neighbor samples are used to interpolate synthetic feature values to retrieve a certain
predefined percentage of additional synthetic samples (over-sampling).cApplying this algorithm to the samples of the
training datasets that belong to the minority classes finally ends up with a more balanced training set and the removal
of the model bias.

## Integration

<img src="https://github.com/predictiveworks/cdap-spark/blob/master/works-ml/images/works-ml.svg" width="95%" alt="Works ML">
