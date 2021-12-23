
# Date Matcher

## Description
This machine learning plugin represents a transformation stage that reads different forms of date and time 
expressions and converts them to a provided date format. 

This stage transforms each text document into a list of sentences where each detected date and time expression 
is replaced by the provided format. As an alternative, the list of detected date and time expressions is returned.

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Data Configuration
**Text Field**: The name of the field in the input schema that contains the text document.

**Date Field**: The name of the field in the output schema that contains the text matches.

### Parameter Configuration
**Date Format**: The expected output date format. Default is 'yyyy/MM/dd'.

**Output Option**: An option to determine how to format the output of the date matcher. Supported values 
are 'extract' and 'replace'. Default is 'replace'.
