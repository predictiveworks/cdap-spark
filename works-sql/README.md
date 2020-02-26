# Works SQL

## Overview
Many excellent complex event processing solutions exist to bring SQL queries to real-time event streams.
Confluent's <a href="https://www.confluent.io/product/ksql">KSQL</a> is certainly one of them.

All these solutions focus on a certain part of data processing spectrum.

What if you want to combine data aggregation, grouping and filtering with a trained machine learning model
in the same data pipeline?

This plugin supports the application of SQL queries to aggregate, group or filter incoming data records. **Works SQL** can be used with *batch* and *stream* processing. 

Suppose you decide to leverage Apacka Kafka as your streaming data connector and starting point of a real-time data pipeline, then *Works SQL* is an easy-to-use and powerful SQL engine for more complex event processing without the need to write code in a certain programming language.

## Integration

<img src="https://github.com/predictiveworks/cdap-spark/blob/master/works-sql/images/works-sql.svg" width="95%" alt="Works SQL">
