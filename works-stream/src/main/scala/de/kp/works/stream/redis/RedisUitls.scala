package de.kp.works.stream.redis
/*
 * Copyright (c) 2021 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 *
 */

import scala.reflect.ClassTag

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext

import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.api.java.{ JavaDStream, JavaReceiverInputDStream, JavaStreamingContext }

import com.redislabs.provider.redis.streaming._

object RedisUitls {
  
  /********** JAVA **********/

  /**
   * Storage level of the data will be the default
   * StorageLevel.MEMORY_AND_DISK_SER_2.
   *
   * @param jssc      		JavaStreamingContext object
   * @param consumersCfg  Sequence of Redis Streams consumer configurations
   * 
   */
  def createStream(
    jssc: JavaStreamingContext,
    consumersCfg: Array[ConsumerConfig]): JavaReceiverInputDStream[RedisStreamItem] = {

    implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[String]]
    createStream(jssc.ssc, consumersCfg)

  }

  /**
   *
   * @param jssc      		JavaStreamingContext object
   * @param storageLevel  RDD storage level.
   * @param redisHost			 Host of the Redis instance
   * @param redisPort     Port of the Redis instance	
   * @param redisSsl			 Flag to indicate SSL connection 
   * @param consumersCfg  Sequence of Redis Streams consumer configurations
   * 
   */
  def createStream(
    jssc: JavaStreamingContext,
    storageLevel: StorageLevel,
    redisHost:String,
    redisPort:Int,
    redisSsl:Boolean,
    consumersCfg: Array[ConsumerConfig]): JavaReceiverInputDStream[RedisStreamItem] = {

    implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[String]]
    createStream(
        jssc.ssc,
        storageLevel,
        redisHost,
        redisPort,
        redisSsl,
        consumersCfg)

  }

  /**
   *
   * @param jssc      		JavaStreamingContext object
   * @param storageLevel  RDD storage level.
   * @param redisHost			 Host of the Redis instance
   * @param redisPort     Port of the Redis instance	
   * @param redisAuth     Password to access Redis instance
   * @param redisSsl			 Flag to indicate SSL connection 
   * @param consumersCfg  Sequence of Redis Streams consumer configurations
   * 
   */
  def createStream(
    jssc: JavaStreamingContext,
    storageLevel: StorageLevel,
    redisHost:String,
    redisPort:Int,
    redisAuth:String,
    redisSsl:Boolean,
    consumersCfg: Array[ConsumerConfig]): JavaReceiverInputDStream[RedisStreamItem] = {

    implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[String]]
    createStream(
        jssc.ssc,
        storageLevel,
        redisHost,
        redisPort,
        redisAuth,
        redisSsl,
        consumersCfg)

  }

  /**
   *
   * @param jssc      		JavaStreamingContext object
   * @param storageLevel  RDD storage level.
   * @param redisHost			 Host of the Redis instance
   * @param redisPort     Port of the Redis instance	
   * @param redisAuth     Password to access Redis instance
   * @param redisDbNum		Redis database number
   * @param redisTimeout  Redis timeout
   * @param redisSsl			 Flag to indicate SSL connection 
   * @param consumersCfg  Sequence of Redis Streams consumer configurations
   * 
   */
  def createStream(
    jssc: JavaStreamingContext,
    storageLevel: StorageLevel,
    redisHost:String,
    redisPort:Int,
    redisAuth:String,
    redisDbNum:Int,
    redisTimeout:Int, 
    redisSsl:Boolean,
    consumersCfg: Array[ConsumerConfig]): JavaReceiverInputDStream[RedisStreamItem] = {

    implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[String]]
    createStream(
        jssc.ssc,
        storageLevel,
        redisHost,
        redisPort,
        redisAuth,
        redisDbNum,
        redisTimeout,
        redisSsl,
        consumersCfg)

  }

  /**
   *
   * @param jssc      						JavaStreamingContext object
   * @param storageLevel  				RDD storage level.
   * @param redisHost							Host of the Redis instance
   * @param redisPort     				Port of the Redis instance	
   * @param redisAuth     				Password to access Redis instance
   * @param redisDbNum						Redis database number
   * @param redisTimeout  				Redis timeout
   * @param redisMaxPipelineSize	Maximum number of Redis pipelines
   * @param redisScanCount				Number of Redis Scan counts
   * @param redisSsl			 				Flag to indicate SSL connection 
   * @param consumersCfg  				Sequence of Redis Streams consumer configurations
   * 
   */
  def createStream(
    jssc: JavaStreamingContext,
    storageLevel: StorageLevel,
    redisHost:String,
    redisPort:Int,
    redisAuth:String,
    redisDbNum:Int,
    redisTimeout:Int, 
    redisMaxPipelineSize:Int,
    redisScanCount:Int,    
    redisSsl:Boolean,
    consumersCfg: Array[ConsumerConfig]): JavaReceiverInputDStream[RedisStreamItem] = {

    implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[String]]
    createStream(
        jssc.ssc,
        storageLevel,
        redisHost,
        redisPort,
        redisAuth,
        redisDbNum,
        redisTimeout,
        redisMaxPipelineSize,
        redisScanCount,
        redisSsl,
        consumersCfg)

  }
  
  /********** SCALA **********/

  /**
   * @param ssc           StreamingContext object
   * @param storageLevel  RDD storage level.
   * @param redisHost			 Host of the Redis instance
   * @param redisPort     Port of the Redis instance	
   * @param redisSsl			 Flag to indicate SSL connection 
   * @param consumersCfg  Sequence of Redis Streams consumer configurations
   */
  def createStream(
    ssc: StreamingContext,
    consumersCfg: Seq[ConsumerConfig]): ReceiverInputDStream[RedisStreamItem] = {
    /*
     * Ths is the minimal configuration of the respective
     * InputDStream that uses almost all default settings
     */
    new RedisInputDStream(
        _ssc = ssc, 
        storageLevel = StorageLevel.MEMORY_AND_DISK_SER_2, 
        consumersCfg = consumersCfg) 
  }

  /**
   * @param ssc           StreamingContext object
   * @param storageLevel  RDD storage level.
   * @param redisHost			 Host of the Redis instance
   * @param redisPort     Port of the Redis instance	
   * @param redisSsl			 Flag to indicate SSL connection 
   * @param consumersCfg  Sequence of Redis Streams consumer configurations
   */
  def createStream(
    ssc: StreamingContext,
    storageLevel: StorageLevel,
    redisHost:String,
    redisPort:Int,
    redisSsl:Boolean,
    consumersCfg: Seq[ConsumerConfig]): ReceiverInputDStream[RedisStreamItem] = {

    new RedisInputDStream(
        _ssc = ssc, 
        storageLevel = storageLevel,
        redisHost = Option(redisHost),
        redisPort = Option(redisPort),
        redisSsl  = redisSsl, 
        consumersCfg = consumersCfg) 
  
  }

  /**
   * @param ssc           StreamingContext object
   * @param storageLevel  RDD storage level. Defaults to StorageLevel.MEMORY_AND_DISK_SER_2.
   * @param redisHost			 Host of the Redis instance
   * @param redisPort     Port of the Redis instance	
   * @param redisAuth     Password to access Redis instance
   * @param redisSsl			 Flag to indicate SSL connection 
   * @param consumersCfg  Sequence of Redis Streams consumer configurations
   */
  def createStream(
    ssc: StreamingContext,
    storageLevel: StorageLevel,
    redisHost:String,
    redisPort:Int,
    redisAuth: String,
    redisSsl:Boolean,
    consumersCfg: Seq[ConsumerConfig]): ReceiverInputDStream[RedisStreamItem] = {

    new RedisInputDStream(
        _ssc = ssc, 
        storageLevel = storageLevel,
        redisHost = Option(redisHost),
        redisPort = Option(redisPort),
        redisAuth = Option(redisAuth),
        redisSsl  = redisSsl, 
        consumersCfg = consumersCfg) 
  
  }

  /**
   * @param ssc           StreamingContext object
   * @param storageLevel  RDD storage level. Defaults to StorageLevel.MEMORY_AND_DISK_SER_2.
   * @param redisHost			 Host of the Redis instance
   * @param redisPort     Port of the Redis instance	
   * @param redisAuth     Password to access Redis instance
   * @param redisDbNum		Redis database number
   * @param redisTimeout  Redis timeout
   * @param redisSsl			Flag to indicate SSL connection 
   * @param consumersCfg  Sequence of Redis Streams consumer configurations
   */
  def createStream(
    ssc: StreamingContext,
    storageLevel: StorageLevel,
    redisHost:String,
    redisPort:Int,
    redisAuth: String,
    redisDbNum:Int, 
    redisTimeout:Int,
    redisSsl:Boolean,
    consumersCfg: Seq[ConsumerConfig]): ReceiverInputDStream[RedisStreamItem] = {

    new RedisInputDStream(
        _ssc = ssc, 
        storageLevel = storageLevel,
        redisHost = Option(redisHost),
        redisPort = Option(redisPort),
        redisAuth = Option(redisAuth),
        redisDbNum = Option(redisDbNum),
        redisTimeout = Option(redisTimeout),
        redisSsl = redisSsl,
        consumersCfg = consumersCfg) 
  
  }

  /**
   * @param ssc           				StreamingContext object
   * @param storageLevel  				RDD storage level. Defaults to StorageLevel.MEMORY_AND_DISK_SER_2.
   * @param redisHost			 				Host of the Redis instance
   * @param redisPort     				Port of the Redis instance	
   * @param redisAuth    					Password to access Redis instance
   * @param redisDbNum						Redis database number
   * @param redisTimeout  				Redis timeout
   * @param redisMaxPipelineSize	Maximum number of Redis pipelines
   * @param redisScanCount				Number of Redis Scan counts
   * @param redisSsl							Flag to indicate SSL connection 
   * @param consumersCfg  				Sequence of Redis Streams consumer configurations
   */
  def createStream(
    ssc: StreamingContext,
    storageLevel: StorageLevel,
    redisHost:String,
    redisPort:Int,
    redisAuth: String,
    redisDbNum:Int, 
    redisTimeout:Int,
    redisMaxPipelineSize:Int,
    redisScanCount:Int,
    redisSsl:Boolean,
    consumersCfg: Seq[ConsumerConfig]): ReceiverInputDStream[RedisStreamItem] = {

    new RedisInputDStream(
        _ssc = ssc, 
        storageLevel = storageLevel,
        redisHost = Option(redisHost),
        redisPort = Option(redisPort),
        redisAuth = Option(redisAuth),
        redisDbNum = Option(redisDbNum),
        redisTimeout = Option(redisTimeout),
        redisMaxPipelineSize = Option(redisMaxPipelineSize),
        redisScanCount = Option(redisScanCount),
        redisSsl = redisSsl,
        consumersCfg = consumersCfg) 
  
  }
    
}