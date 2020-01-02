
# Apply Rules

Description
-----------

This data analytics machine supports the application of Drools compliant business rules to data records within your data pipeline. Besides data discovery and machine learning, for certain use cases there still is a need to filter or score datasets with user-defined business rules.

Drools ships with a business-ready rule engine and this data machine offers easy access to this engine by defining ad hoc business rules.  

Properties
----------

**Reference Name:** The name used to uniquely identify this analytics machine for data lineage.

**Drools Ruleset:** This field contains the specification of a set of Drools compliant rules as described in the example section.

**Ruleset Type:** This data machine supports two different use cases. 

* _Filtering_ restricts the data records to those that match the defined ruleset. It is configured by selecting 'filter'. 

* _Scoring_ assigns an extra score field to matching datasets to express e.g. the relevance of certain rule with respect to your business context. This use case can be configured by selecting 'score'.

**It is important to you do not mix rules of different types within a ruleset.**

Refer to the example section to learn the difference between filter & scoring rules. It is important 

Examples
--------

A Drools ruleset consists of the subsequent sections _rule_, _when_, _then_ and _end_.

**rule:** This is the initial section of each rule and specifies its unique name within a set of rules.

**when:** This section defines the condition of each rule. It is important to start with the specification of the associated
fact and its attributes, ```DroolsFact($fact:fact)``` before the matching condition ```eval(...)``` is defined. 

**then:** This section determines how to proceed when a certain fact matches the filter or score condition. The syntax is pre-defined. 

For filter rules please use
```code
droolsResults.add(new DroolsResult($fact));
```
and for scoring rules
```code
droolsResults.add(new DroolsResult($fact,"score",0.25));
```
with a user-defined name (e.g. "score") of the score field and score value (e.g. 0.25).

**end:** This section defines the end of a certain rule within a set of rules.

**Filter rule:**
```code
rule "filterRule"
 
when
 
  DroolsFact($fact:fact)
  eval(((String)$fact.get("field1")).equals("1313361152") && ((Integer)$fact.get("field2")) >= 20)
 
then
  droolsResults.add(new DroolsResult($fact));
   
 end
```
**Scoring rule:**
```code
rule "scoreRule"
 
when
 
  DroolsFact($fact:fact)
  eval(((String)$fact.get("field1")).equals("1313361152") && ((Integer)$fact.get("field2")) >= 20)
 
then
  droolsResults.add(new DroolsResult($fact,"score",0.25));
   
end
``` 

