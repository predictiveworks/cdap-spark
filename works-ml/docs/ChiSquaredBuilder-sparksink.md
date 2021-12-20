
# Chi-Squared Builder

## Description

This machine learning plugin represents the building stage for an Apache Spark ML "Chi-Squared Selector model".

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Model Configuration
**Model Name**: The unique name of the machine learning model.

**Model Stage***: The stage of the ML model. Supported values are 'experiment', 'staging', 'production'
and 'archived'. Default is 'experiment'.

### Data Configuration
**Input Field**: The name of the field in the input schema that contains the features to build the model from.

**Label Field**: The name of the field in the input schema that contains the label.

### Parameter Configuration
**Selector Type**: The selector type. Supported values: 'numTopFeatures, 'percentile, and 'fpr'. Default is 
'numTopFeatures'.

**Top Features**: The number of features that selector will select, ordered by ascending p-value. The number 
of features is less than this parameter value, then this will select all features.  Only applicable when 
the selector type = 'numTopFeatures'. Default value is 50.

**Percentile of Features**: Percentile of features that selector will select, ordered by statistics value descending.
Only applicable when the selector type = 'percentile'. Must be in range (0, 1). Default value is 0.1.

**Highest P-Value**: The highest p-value for features to be kept. Only applicable when the select type = 'fpr'.
Must be in range (0, 1). Default value is 0.05.
