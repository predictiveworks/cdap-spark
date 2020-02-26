# Works Text

## Overview
PredictiveWorks. externalizes John Snow Labs' <a href="https://https://nlp.johnsnowlabs.com/">Spark NLP</a>
library as standardized pipeline plugins for natural language processing.

NLP tasks, such a Dependency Parsing, Named Entity Recognition, Part of Speech Tagging, or
Sentiment Analysis are available as plugins with the ability of a seamless combination
with any other plugin.  

## Motivation

>According to reliable industry estimates, less than 25% of the available data is present in structured form.
Data is being generated as we speak, tweet, send messages and in various other activities. The majority exists
in the textual form, which is highly unstructured in nature.

Information present in unstructured text is not directly accessible unless it is processed.

Natural Language Processing (NLP) is an instrument to produce significant and actionable insights from text. What "text" means,
is a matter of context and imagination and is not restricted to news articles, social media posts or user product reviews. In
cyber security, for example, NLP can be used to tackle problems such as:

* <b>Domain Generation Algorithm classification</b>: Identification of malicious domains (blbwpvcyztrepfue.ru)

* <b>Source Code Vulnerability Analysis</b>: Determining function patterns associated with known vulnerabilities to identify other potentially vulnerable code segments.

* <b>Phishing Identification</b>: A bag of words model determines the probability an email message contains a phishing attempt or not.

* <b>Malware Family Analysis</b>: Topic modeling assigns samples of malware to "their" families.

## Why Spark NLP?

<b>Spark NLP</b> is a production grade library that is in use in many Fortune 500's even in
mission critical environments such as healthcare. Speed, scale and accuracy are unmatched when
  compared to other excellent NLP libraries.

The library covers many common NLP tasks, including tokenization, stemming, lemmatization, part of speech tagging,
sentiment analysis, spell checking, named entity recognition, and more.

Spark NLP is built on the shoulders of Apache Spark ML and Tensorflow, and shares the vision of
standardized natural language processing.

## NLP Tasks

### Preprocessing
PredictiveWorks. comes with a variety of text analysis plugins, but the very beginning of each text processing pipeline is 
characterized by more fundamental steps such as detecting sentence boundaries, extracting terms or tokens and normalizing them.

### Dependency Parsing
The dependency parsing (unlabeled) scope of Spark NLP is provided as standardized model building
and tagging plugins that share the same technology.    

Trained and retrained syntactic grammar models are immediately available for production pipelines.

### Named Entity Recognition
The named entity recognition scope of Spark NLP is provided as standardized model building
and tagging plugins that share the same technology. It is complemented by an extra plugin that
leverages co-occurrences of detected entities to determine interesting relations between them.   

Features:
* Named Entity Tagging
* Named Entity Relations

Trained and retrained Named Entity Recognition models are immediately available for production
pipelines.



### Noise Reduction
Any piece of text which is not relevant to the context of the data can be specified as noise.
Noise comprises commonly used but less relevant words of a language, misspellings, multiple
variations of word representation which all reduce to the same semantic context.

PredictiveWorks. offers standardized plugins for model building
and transformation (sharing the same technology) and thereby covers all relevant facets of noise
reduction: 
* Lemmatization 
* Spell Correction
* Stemming 
* Stopword Removal

Trained and retrained noise reduction models are immediately available for production pipelines.

###Part of Speech Analysis
The part-of-speech scope of Spark NLP is externalized as standardized model building, chunking
and tagging plugins that all share the same technology.

Part of Speech analysis preserves different contexts of a certain word (other than pure bag of
word models) and helps to build stronger word features.

Trained and retrained part-of-speech models are immediately available for production pipelines.

### Sentiment Analysis
The sentiment analysis scope of Spark NLP is externalized as standardized model building and
prediction plugins that share the same technology.

Trained and retrained sentiment models are immediately available for production pipelines.

### Text Engineering
Predictive<span class="brand-teal">Works.</span> supports two flavors of text transforming
for subsequent model building, prediction or tagging tasks. One is mapping words, sentences or even
documents into a vector space to benefit from geometry-induced model building.

The other flavor addresses the space below word boundary and splits each word into its n-grams to add
this dimension model building, prediction or tagging tasks.

Feature:
* NGram Tokenization
* Sentence Embedding
* Word Embedding

### Text Matching
The text matching scope of Spark NLP is externalized as standardized matching plugins. Text matching is
rule-based and does not need any model building. Supported matching: 
* Date
* Phrase
* Regex

### Topic Modeling
Predictive<span class="brand-teal">Works.</span> complements Spark-NLP's functional scope with
standardized plugins for topic clustering & description. Again, model building and prediction
plugins share the same technology.

Trained and retrained topic models are immediately available for production pipelines.

## Integration
The main objective of PredictiveWorks. text analysis support is to make [John Snow Lab's](https://nlp.johnsnowlabs.com/) **Spark NLP** available as plugin components for [CDAP](https://cdap.io) data pipelines.

<img src="https://github.com/predictiveworks/cdap-spark/blob/master/works-text/images/works-text.svg" width="95%" alt="Works Text">
