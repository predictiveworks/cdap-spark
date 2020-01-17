<img src="https://github.com/predictiveworks/cdap-spark/blob/master/images/works-rules.svg" width="360" alt="Works Rules"> 

# Works Rules  

Apache Spark based deep learning & machine learning can respond to wide variety of problems and its ability to discover patterns seem to lead to a complete replacement of business rule applications.

Nowadays business rules seem to be oudated, but what If you need to add extra fields to a real-time event based on a pre-defined and reusable condition. This is a sample of a use case, where facts (e.g. event properties) are transformed into derived facts.

A [Drools](https://drools.org) rule engine takes a declarative definition of a set of (ad-hoc) business rules as input and
transforms data records onto the declared output. 

Suppose a machine learning data pipeline has been used to identify anomalies in a certain dataset. Then the characteristics of these anomalies can be specified as a set of business rules with a certain output characteristics (e.g. an alert.)

**Works Rules** integrates with Drools rule engine and makes it available as a [CDAP](https://cdap.io) data pipeline plugin to transform either data records of historical datasets or real-time events of a data stream.

<img src="https://github.com/predictiveworks/cdap-spark/blob/master/works-rules/images/works-rules.png" width="800" alt="Works Rules">
