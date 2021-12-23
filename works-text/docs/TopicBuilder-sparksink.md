
# Topic Builder

## Description
This machine learning plugin represents the building stage for an Apache Spark ML "Latent Dirichlet Allocation 
(LDA) model". In contrast to the LDA Builder plugin, this stage leverages an implicit document vectorization 
based on the term counts of the provided corpus. The trained model can be used to either determine the topic 
distribution per document or term-distribution per topic.

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
**Topics**: The number of topics that have to be created. Default is 10.

**Maximum Iterations**: The (maximum) number of iterations the algorithm has to execute. Default value is 20.

**Vocabulary Size**: The size of the vocabulary to build vector representations. Default is 10000.