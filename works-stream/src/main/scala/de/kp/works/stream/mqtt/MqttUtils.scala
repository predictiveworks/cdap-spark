package de.kp.works.stream.mqtt
/*
 * Copyright (c) 2020 Dr. Krusche & Partner PartG. All rights reserved.
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
import org.apache.spark.streaming.api.java.{JavaDStream, JavaReceiverInputDStream, JavaStreamingContext}
import org.apache.spark.streaming.dstream.ReceiverInputDStream

import de.kp.works.stream.creds._

object MqttUtils {

  /********** JAVA **********/
  
  /**
   * Storage level of the data will be the default 
   * StorageLevel.MEMORY_AND_DISK_SER_2.
   *
   * @param jssc      JavaStreamingContext object
   * @param brokerUrl Url of remote MQTT publisher
   * @param topics    Array of topic names to subscribe to
   */
  def createStream(
      jssc: JavaStreamingContext,
      brokerUrl: String,
      topics: Array[String]
    ): JavaReceiverInputDStream[MqttResult] = {
    
    implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[String]]
    createStream(jssc.ssc, brokerUrl, topics)
    
  }

  /**
   * @param jssc         JavaStreamingContext object
   * @param brokerUrl    Url of remote MQTT publisher
   * @param topics       Array of topic names to subscribe to
   * @param storageLevel RDD storage level.
   */
  def createStream(
      jssc: JavaStreamingContext,
      brokerUrl: String,
      topics: Array[String],
      storageLevel: StorageLevel
    ): JavaReceiverInputDStream[MqttResult] = {
    
    implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[String]]
    createStream(jssc.ssc, brokerUrl, topics, storageLevel)
  
  }

  /**
   * @param jssc              JavaStreamingContext object
   * @param brokerUrl         Url of remote MQTT publisher
   * @param topics            Array of topic names to subscribe to
   * @param storageLevel      RDD storage level.
   * @param clientId          ClientId to use for the mqtt connection
   * @param credentials       User credentials for authentication to the mqtt publisher
   * @param cleanSession      Sets the mqtt cleanSession parameter
   * @param qos               Quality of service to use for the topic subscription
   * @param connectionTimeout Connection timeout for the mqtt connection
   * @param keepAliveInterval Keepalive interal for the mqtt connection
   * @param mqttVersion       Version to use for the mqtt connection
   */
  def createStream(
      jssc: JavaStreamingContext,
      brokerUrl: String,
      topics: Array[String],
      storageLevel: StorageLevel,
      clientId: String,
      credentials: Credentials,
      cleanSession: Boolean,
      qos: Int,
      connectionTimeout: Int,
      keepAliveInterval: Int,
      mqttVersion: Int
    ): JavaReceiverInputDStream[MqttResult] = {
    
    implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[String]]
    createStream(jssc.ssc, brokerUrl, topics, storageLevel, Option(clientId),
        Option(credentials), Option(cleanSession), Option(qos),
        Option(connectionTimeout), Option(keepAliveInterval), Option(mqttVersion))
  }

  /**
   * @param jssc              JavaStreamingContext object
   * @param brokerUrl         Url of remote MQTT publisher
   * @param topics            Array of topic names to subscribe to
   * @param clientId          ClientId to use for the mqtt connection
   * @param credentials       User credentials for authentication to the mqtt publisher
   * @param cleanSession      Sets the mqtt cleanSession parameter
   * @param qos               Quality of service to use for the topic subscription
   * @param connectionTimeout Connection timeout for the mqtt connection
   * @param keepAliveInterval Keepalive interal for the mqtt connection
   * @param mqttVersion       Version to use for the mqtt connection
   */
  def createStream(
      jssc: JavaStreamingContext,
      brokerUrl: String,
      topics: Array[String],
      clientId: String,
      credentials: Credentials,
      cleanSession: Boolean,
      qos: Int,
      connectionTimeout: Int,
      keepAliveInterval: Int,
      mqttVersion: Int
    ): JavaReceiverInputDStream[MqttResult] = {
    
    implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[String]]
    createStream(jssc.ssc, brokerUrl, topics, StorageLevel.MEMORY_AND_DISK_SER_2,
      Option(clientId), Option(credentials), Option(cleanSession), Option(qos),
      Option(connectionTimeout), Option(keepAliveInterval), Option(mqttVersion))
      
  }

  /**
   * @param jssc         JavaStreamingContext object
   * @param brokerUrl    Url of remote MQTT publisher
   * @param topics       Array of topic names to subscribe to
   * @param clientId     ClientId to use for the mqtt connection
   * @param credentials  User credentials for authentication to the mqtt publisher
   * @param cleanSession Sets the mqtt cleanSession parameter
   */
  def createPairedStream(
      jssc: JavaStreamingContext,
      brokerUrl: String,
      topics: Array[String],
      clientId: String,
      credentials: Credentials,
      cleanSession: Boolean
      ): JavaReceiverInputDStream[MqttResult] = {
    
    implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[String]]
    createStream(jssc.ssc, brokerUrl, topics, StorageLevel.MEMORY_AND_DISK_SER_2,
      Option(clientId), Option(credentials), Option(cleanSession), None,
      None, None, None)
      
  }

  /********** SCALA **********/
  
  /**
   * @param ssc           StreamingContext object
   * @param brokerUrl     Url of remote MQTT publisher
   * @param topics        Array of topic names to subscribe to
   * @param storageLevel  RDD storage level. Defaults to StorageLevel.MEMORY_AND_DISK_SER_2.
   */
  def createStream(
      ssc: StreamingContext,
      brokerUrl: String,
      topics: Array[String],
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
    ): ReceiverInputDStream[MqttResult] = {
    new MqttInputDStream(ssc, brokerUrl, topics, storageLevel)
  }

 /**
  * @param ssc               StreamingContext object
  * @param brokerUrl         Url of remote MQTT publisher
  * @param topics            Array of topic names to subscribe to
  * @param storageLevel      RDD storage level. Defaults to StorageLevel.MEMORY_AND_DISK_SER_2.
  * @param clientId          ClientId to use for the mqtt connection
  * @param credentials       User credentials for authentication to the mqtt publisher
  * @param cleanSession      Sets the mqtt cleanSession parameter
  * @param qos               Quality of service to use for the topic subscription
  * @param connectionTimeout Connection timeout for the mqtt connection
  * @param keepAliveInterval Keepalive interal for the mqtt connection
  * @param mqttVersion       Version to use for the mqtt connection
  */
  def createStream(
      ssc: StreamingContext,
      brokerUrl: String,
      topics: Array[String],
      storageLevel: StorageLevel,
      clientId: Option[String],
      credentials: Option[Credentials],
      cleanSession: Option[Boolean],
      qos: Option[Int],
      connectionTimeout: Option[Int],
      keepAliveInterval: Option[Int],
      mqttVersion: Option[Int]
    ): ReceiverInputDStream[MqttResult] = {
    new MqttInputDStream(ssc, brokerUrl, topics, storageLevel, clientId, credentials,
      cleanSession, qos, connectionTimeout, keepAliveInterval, mqttVersion)
  }

}
