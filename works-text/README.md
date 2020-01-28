<p align="center">
<img src="https://github.com/predictiveworks/cdap-spark/blob/master/images/works-text.svg" width="360" alt="Works Text"> 
</p>

# 
This project aims to implement the vision of **Visual NLP** - Code-free orchestration of data pipelines (or workflow) to respond to natural language processing use cases.

>Do not reinvent the wheel again

**Works Text** integrates [John Snow Lab's](https://nlp.johnsnowlabs.com/) excellent **Spark NLP** library with [Google CDAP](https://cdap.io) and offers approved NLP features as plugins for CDAP data pipelines.

<img src="https://github.com/predictiveworks/cdap-spark/blob/master/works-text/images/works-text.png" width="800" alt="Works Text">

The following NLP features are externalized as pipeline plugins:

* Bert Embeddings
* Chunk Embedding
* Chunking
* Dependency Parsing
* Lemmatizer
* NGrams
* Named Entity Recognition
* Normalizer
* Part of Speech Tagging
* Sentence Detection
* Sentiment Detection
* Spell Checking
* Stemmer
* Stop Words Removal
* Text Matching
* Tokenization
* Word Embedding (GloVe and Word2Vec)

## Matching

| Plugin | Description | Plugin Type
| --- | --- | --- |
| DateMatcher | This plugin detects a variety of different forms of date and time expressions and converts them into a provided date format. | SparkCompute
| RegexMatcher | This plugin applies a set of regular expression rules and detects a list of chunks that match these rules.| SparkCompute 