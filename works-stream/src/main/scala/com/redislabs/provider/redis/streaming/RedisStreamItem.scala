package com.redislabs.provider.redis.streaming
/*
 * This class is extracted from the spark-redis
 * project to downgrade for use with Spark 2.1.3
 * 
 * The case class `StreamItem` was re-factored 
 * into the class `RedisStreamItem`
 */

/**
 * Represent an item in the stream
 *
 * @param streamKey stream key
 * @param id        item(entry) id
 * @param fields    key/value map of item fields
 */
class RedisStreamItem(streamKey: String, id: ItemId, fields: Map[String, String])
