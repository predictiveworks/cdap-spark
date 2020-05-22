
# Works Stream

This library leverages Apache Spark streaming to expose real-time event streams from multiple 
data sources.

## JDBC Streaming

Coming soon

## MQTT Streaming

MQTT streaming supports real-time events streams originating from MQTT brokers. For common use cases, the Eclipse Paho client for MQTT v3 is used. 

For specialized use cases, e.g. in the automotive industry, also the **HiveMQ** client for v3 and v5 is supported.

MQTT messages are enriched with semantic and temporal meta information to enable early correlation of MQTT events.
