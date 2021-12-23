
# LDA Text Builder

## Description
This machine learning plugin represents the building stage for a Spark ML "Latent Dirichlet Allocation (LDA) model". 
An LDA model can be used for text clustering or labeling. This model training stage requires a pre-trained Word 
Embedding model.

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Model Configuration
**Model Name**: The unique name of the machine learning model.

**Model Stage***: The stage of the ML model. Supported values are 'experiment', 'staging', 'production'
and 'archived'. Default is 'experiment'.

### Data Configuration
**Text Field**: The name of the field in the input schema that contains the text document.

**Data Split**: The split of the dataset into train & test data, e.g. 80:20. Default is 90:10.

### Parameter Configuration
**Embedding Name**: The unique name of the trained Word2Vec embedding model.

**Embedding Stage**: The stage of the Word2Vec embedding model. Supported values are 'experiment', 'staging',
'production' and 'archived'. Default is 'experiment'.

**Clusters**: The number of topics that have to be created. Default is 10.

**Maximum Iterations**: The (maximum) number of iterations the algorithm has to execute. Default value is 20.

**Pooling Strategy**: The pooling strategy how to merge word embeddings into document embeddings. Supported 
values are 'average' and 'sum'. Default is 'average'.
