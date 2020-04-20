
# Apache Ignite & Spark Streaming for Eclipse Ditto

Eclipse Ditto is a technology in the IoT implementing a software pattern called **digital twins**.
A digital twin is a virtual, cloud based, representation of his real world counterpart (real world 
Things, e.g. devices like sensors, smart heating, connected cars, smart grids, EV charging stations etc).

The technology mirrors potentially millions and billions of digital twins residing in the digital world with physical **Things**. 
This simplifies developing IoT solutions for software developers as they do not need to know how or where exactly the physical 
Things are connected.

With Ditto a thing can just be used as any other web service via its digital twin.

# Historical Data & Eclipse Ditto

AFAIK, in Eclipse Ditto, you cannot retrieve historical data. Ditto is about representing the current state of the digital twin, 
and for communicating directly with the real device. **Historical values are not persisted in Ditto**.

Those of us, who have the need to access historical device data (which is a very normal requirement, say for anomaly detection) are
often told to add a connection in Ditto to Apache Kafka to expose twin change events, and e.g. consume in a time series database like
InfluxDB.

InfluxDB is a great time series database, but we serious doubts that this database is ready for IoT-scale data. What about an alternative recommendation?

The Ditto data streamer for Apache Ignite can be used to feed twin change events and live messages into Apache Ignite caches.
This is the only intermediate step before Apache Ignite's SQL and ML power can directly be applied to telemetry data.

And CrateDB is an appropriate persistence layer for Apache Ignite. More advanced that e.g. Apache Cassandra.


