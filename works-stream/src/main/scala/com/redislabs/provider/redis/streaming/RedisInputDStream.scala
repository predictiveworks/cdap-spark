package com.redislabs.provider.redis.streaming
/*
 * This class is extracted from the spark-redis
 * project to downgrade for use with Spark 2.1.3
 */

import java.util.AbstractMap.SimpleEntry

import org.apache.curator.utils.ThreadUtils

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.dstream.ReceiverInputDStream

import org.spark_project.guava.util.concurrent.RateLimiter

import com.redislabs.provider.redis.{RedisConfig, ReadWriteConfig}
import com.redislabs.provider.redis.util.{Logging, PipelineUtils, StreamUtils}

import redis.clients.jedis.{StreamEntryID, Jedis, Protocol, StreamEntry}

import scala.collection.JavaConversions._

/**
  * Receives messages from Redis Stream
  */
class RedisInputDStream(
  _ssc: StreamingContext,
  storageLevel: StorageLevel,
  /*
   * Redis Configuration 
   */
  redisHost:Option[String] = None,
  redisPort:Option[Int]    = None,
  redisAuth:Option[String] = None,
  redisDbNum:Option[Int]   = None,
  redisTimeout:Option[Int] = None,
  /*
   * Redis Read/Write Configuration
   */
  redisMaxPipelineSize:Option[Int] = None,
  redisScanCount:Option[Int]       = None,
  /*
   * Flag to indicate whether to use redis:// 
   * or rediss:// protocol
   */
  redisSsl:Boolean = false,
  /*
   * Configuration of the consumers of the
   * Redis Stream
   */
  consumersCfg: Seq[ConsumerConfig]) extends ReceiverInputDStream[RedisStreamItem](_ssc) {  
  /*
   * Aggregate provided Redis configuration
   */
  val conf = new SparkConf

  val host = redisHost.getOrElse(Protocol.DEFAULT_HOST)
  conf.set("spark.redis.host", host)

  val port = redisPort.getOrElse(Protocol.DEFAULT_PORT)
  conf.set("spark.redis.port", port.toString)
  
  val auth = redisAuth.getOrElse(null)
  conf.set("spark.redis.auth", auth)      

  val db = redisDbNum.getOrElse(Protocol.DEFAULT_DATABASE)
  conf.set("spark.redis.db", db.toString)
  
  val timeout = redisTimeout.getOrElse(Protocol.DEFAULT_TIMEOUT)
  conf.set("spark.redis.timeout", timeout.toString)
  
  val ssl = redisSsl
  conf.set("spark.redis.ssl", ssl.toString)
  /*
   * Aggregate Read/Write Configuration
   */
  val maxPipelineSize = redisMaxPipelineSize.getOrElse(100)
  conf.set("spark.redis.max.pipeline.size", maxPipelineSize.toString)
  
  val scanCount = redisScanCount.getOrElse(100)
  conf.set("spark.redis.scan.count", scanCount.toString)
  
  
  val redisCfg = RedisConfig.fromSparkConf(conf)
  val readWriteCfg = ReadWriteConfig.fromSparkConf(conf)
  
  def getReceiver(): Receiver[RedisStreamItem] = {
    new RedisStreamReceiver(storageLevel, consumersCfg, redisCfg, readWriteCfg)
  }

}

/**
 * Receives messages from Redis Stream
 */
class RedisStreamReceiver(
  storageLevel: StorageLevel,  
  consumersCfg: Seq[ConsumerConfig],
  redisCfg: RedisConfig,
  readWriteConfig: ReadWriteConfig)
  extends Receiver[RedisStreamItem](storageLevel) with Logging {

  override def onStart(): Unit = {
    
    logInfo("Starting Redis Stream Receiver")
    val executorPool = ThreadUtils.newFixedThreadPool(consumersCfg.size, "RedisStreamMessageHandler")
    
    try {

      /* 
       * Start consumers in separate threads 
       */
      for (c <- consumersCfg) {
        executorPool.submit(new MessageHandler(c, redisCfg, readWriteConfig))
      }
    } finally {
      /* 
       * Terminate threads after the work is done 
       */
      executorPool.shutdown()
    }
  }

  override def onStop(): Unit = {
  }

  private class MessageHandler(
    consumerCfg: ConsumerConfig,
    redisCfg: RedisConfig,
    implicit val readWriteConfig: ReadWriteConfig) extends Runnable {

    val jedis: Jedis = redisCfg.connectionForKey(consumerCfg.streamKey)
    val rateLimiterOpt: Option[RateLimiter] = consumerCfg.rateLimitPerConsumer.map(r => RateLimiter.create(r))

    override def run(): Unit = {
      
      logInfo(s"Starting MessageHandler $consumerCfg")
      try {
      
        createConsumerGroupIfNotExist()
        receiveUnacknowledged()
        receiveNewMessages()
      
      } catch {
        case e: Exception =>
          restart("Error handling message. Restarting.", e)
      }
    }

    private def createConsumerGroupIfNotExist(): Unit = {

      val entryId = consumerCfg.offset match {
        case Earliest => new StreamEntryID(0, 0)
        case Latest => StreamEntryID.LAST_ENTRY
        case IdOffset(v1, v2) => new StreamEntryID(v1, v2)
      
      }
      
      StreamUtils.createConsumerGroupIfNotExist(jedis, consumerCfg.streamKey, consumerCfg.groupName, entryId)
    
    }

    private def receiveUnacknowledged(): Unit = {

      logInfo(s"Starting receiving unacknowledged messages for key ${consumerCfg.streamKey}")
      var continue = true
      
      val unackId = new SimpleEntry(consumerCfg.streamKey, new StreamEntryID(0, 0))

      while (!isStopped && continue) {
        val response = jedis.xreadGroup(
          consumerCfg.groupName,
          consumerCfg.consumerName,
          consumerCfg.batchSize,
          consumerCfg.block,
          false,
          unackId)

        val unackMessagesMap = response.map(e => (e.getKey, e.getValue)).toMap
        val entries = unackMessagesMap(consumerCfg.streamKey)
        
        if (entries.isEmpty) {
          continue = false
        }
        
        storeAndAck(consumerCfg.streamKey, entries)
      
      }
    }

    def receiveNewMessages(): Unit = {

      logInfo(s"Starting receiving new messages for key ${consumerCfg.streamKey}")
      val newMessId = new SimpleEntry(consumerCfg.streamKey, StreamEntryID.UNRECEIVED_ENTRY)

      while (!isStopped) {
        val response = jedis.xreadGroup(
          consumerCfg.groupName,
          consumerCfg.consumerName,
          consumerCfg.batchSize,
          consumerCfg.block,
          false,
          newMessId)

        if (response != null) {
          for (streamMessages <- response) {
            val key = streamMessages.getKey
            val entries = streamMessages.getValue
            storeAndAck(key, entries)
          }
        }
      }
    }

    def storeAndAck(streamKey: String, entries: Seq[StreamEntry]): Unit = {
      if (entries.nonEmpty) {
        // limit the rate if it's enabled
        rateLimiterOpt.foreach(_.acquire(entries.size))
        val streamItems = entriesToItems(streamKey, entries)
        // call store(multiple-records) to reliably store in Spark memory
        store(streamItems.iterator)
        // ack redis
        PipelineUtils.foreachWithPipeline(jedis, entries) { (pipeline, entry) =>
          pipeline.xack(streamKey, consumerCfg.groupName, entry.getID)
        }
      }
    }

    def entriesToItems(key: String, entries: Seq[StreamEntry]): Seq[RedisStreamItem] = {
      entries.map { e =>
        val itemId = ItemId(e.getID.getTime, e.getID.getSequence)
        new RedisStreamItem(key, itemId, e.getFields.toMap)
      }
    }
  }

}
