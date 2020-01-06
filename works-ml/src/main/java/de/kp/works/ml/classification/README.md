# Classification

## What is in it for you?

**Works-ML** externalizes the following classification algorithms provided by Apache Spark ML:

* Decision Tree
* Gradient-Boosted Trees
* Logistic Regression
* Multilayer Perceptron
* Naive Bayes
* One Vs. Rest
* Random Forest

Each algorithm is wrapped as a Google CDAP *SparkSink* plugin for model building, and as *SparkCompute* for prediction purposes. This approach externalizes each classification algorithm as a visual pipeline stage and thereby supports visual machine learning without the need to write code in any programming language. 