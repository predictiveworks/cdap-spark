package com.redislabs.provider.redis.streaming
/*
 * This class is extracted from the spark-redis
 * project to downgrade for use with Spark 2.1.3
 */

/**
 * @param streamKey            redis stream key
 * @param groupName            consumer group name
 * @param consumerName         consumer name
 * @param offset               stream offset
 * @param rateLimitPerConsumer maximum retrieved messages per second per single consumer
 * @param batchSize            maximum number of pulled items in a read API call
 * @param block                time in milliseconds to wait for data in a blocking read API call
 */
class ConsumerConfig(
  val streamKey: String,
  val groupName: String,
  val consumerName: String,
  val offset: Offset = Latest,
  val rateLimitPerConsumer: Option[Int] = None,
  val batchSize: Int = 100,
  val block: Long = 500)

/**
  * Represents an offset in the stream
  */
sealed trait Offset

/**
  * Latest offset, known as a '$' special id
  */
case object Latest extends Offset

/**
  * Earliest offset, '0-0' id
  */
case object Earliest extends Offset

/**
  * Specific id in the form of 'v1-v2'
  *
  * @param v1 first token of the id
  * @param v2 second token of the id
  */
case class IdOffset(v1: Long, v2: Long) extends Offset

/**
  * Item id in the form of 'v1-v2'
  *
  * @param v1 first token of the id
  * @param v2 second token of the id
  */
case class ItemId(v1: Long, v2: Long)
