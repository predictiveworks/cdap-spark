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
import org.apache.spark.streaming.api.java.{ JavaDStream, JavaReceiverInputDStream, JavaStreamingContext }
import org.apache.spark.streaming.dstream.ReceiverInputDStream

import de.kp.works.stream.ssl._

object MqttUtils {

  /********** JAVA **********/

  /**
   * Storage level of the data will be the default
   * StorageLevel.MEMORY_AND_DISK_SER_2.
   *
   * @param jssc      JavaStreamingContext object
   * @param brokerUrl Url of remote mqtt publisher
   * @param topics    Array of topic names to subscribe to
   * @param userName	 Name of the mqtt user
   * @param userPass  Password of the mqtt user
   * 
   */
  def createStream(
    jssc: JavaStreamingContext,
    brokerUrl: String,
    topics: Array[String],
    userName: String,
    userPass: String): JavaReceiverInputDStream[MqttEvent] = {

    implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[String]]
    createStream(jssc.ssc, StorageLevel.MEMORY_AND_DISK_SER_2,brokerUrl, topics, userName, userPass)

  }

  /**
   * @param jssc         JavaStreamingContext object
   * @param storageLevel RDD storage level.
   * @param brokerUrl    Url of remote mqtt publisher
   * @param topics       Array of topic names to subscribe to
   * @param userName			Name of the mqtt user
   * @param userPass     Password of the mqtt user
   */
  def createStream(
    jssc: JavaStreamingContext,
    storageLevel: StorageLevel,
    brokerUrl: String,
    topics: Array[String],
    userName: String,
    userPass: String): JavaReceiverInputDStream[MqttEvent] = {

    implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[String]]
    createStream(jssc.ssc, storageLevel, brokerUrl, topics, userName, userPass)

  }

  /**
   * @param jssc              JavaStreamingContext object
   * @param storageLevel      RDD storage level.
   * @param brokerUrl         Url of remote mqtt publisher
   * @param topics            Array of topic names to subscribe to
   * @param userName			     Name of the mqtt user
   * @param userPass          Password of the mqtt user
 	 * @param sslOptions        SSL authentication
   * @param clientId          ClientId to use for the mqtt connection
   * @param cleanSession      Sets the mqtt cleanSession parameter
   * @param qos               Quality of service to use for the topic subscription
   * @param connectionTimeout Connection timeout for the mqtt connection
   * @param keepAliveInterval Keepalive interal for the mqtt connection
   * @param mqttVersion       Version to use for the mqtt connection
   */
  def createStream(
    jssc: JavaStreamingContext,
    storageLevel: StorageLevel,
    brokerUrl: String,
    topics: Array[String],
    userName: String,
    userPass: String,   
    sslOptions: SSLOptions,
    clientId: String,
    cleanSession: Boolean,
    qos: Int,
    connectionTimeout: Int,
    keepAliveInterval: Int,
    mqttVersion: Int): JavaReceiverInputDStream[MqttEvent] = {
    /*
     * The client identifier user for the MQTT connection
     * is an optional parameter
     */
    val client = if (clientId == null || clientId.isEmpty) None else Option(clientId)
    implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[String]]

    createStream(jssc.ssc, storageLevel, brokerUrl, topics, userName, userPass, Option(sslOptions), client,
      Option(cleanSession), Option(qos),
      Option(connectionTimeout), Option(keepAliveInterval), Option(mqttVersion))
  }

  /**
   * @param jssc              JavaStreamingContext object
   * @param brokerUrl         Url of remote mqtt publisher
   * @param topics            Array of topic names to subscribe to
   * @param userName			     Name of the mqtt user
   * @param userPass          Password of the mqtt user
 	 * @param sslOptions        SSL authentication
   * @param clientId          ClientId to use for the mqtt connection
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
    userName: String,
    userPass: String,
    sslOptions: SSLOptions,
    clientId: String,
    cleanSession: Boolean,
    qos: Int,
    connectionTimeout: Int,
    keepAliveInterval: Int,
    mqttVersion: Int): JavaReceiverInputDStream[MqttEvent] = {
    /*
     * The client identifier user for the MQTT connection
     * is an optional parameter
     */
    val client = if (clientId == null || clientId.isEmpty) None else Option(clientId)
    implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[String]]

    createStream(jssc.ssc, StorageLevel.MEMORY_AND_DISK_SER_2, brokerUrl, topics, userName, userPass,
      Option(sslOptions), client, Option(cleanSession), Option(qos),
      Option(connectionTimeout), Option(keepAliveInterval), Option(mqttVersion))

  }

  /**
   * @param jssc         JavaStreamingContext object
   * @param brokerUrl    Url of remote mqtt publisher
   * @param topics       Array of topic names to subscribe to
   * @param userName			Name of the mqtt user
   * @param userPass     Password of the mqtt user
 	 * @param sslOptions   SSL authentication
   * @param clientId     ClientId to use for the mqtt connection
   * @param credentials  User credentials for authentication to the mqtt publisher
   * @param cleanSession Sets the mqtt cleanSession parameter
   */
  def createStream(
    jssc: JavaStreamingContext,
    brokerUrl: String,
    topics: Array[String],
    userName: String,
    userPass: String,
    sslOptions: SSLOptions,
    clientId: String,
    cleanSession: Boolean,
    qos: Int): JavaReceiverInputDStream[MqttEvent] = {
    /*
     * The client identifier user for the MQTT connection
     * is an optional parameter
     */
    val client = if (clientId == null || clientId.isEmpty) None else Option(clientId)
    implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[String]]

    createStream(jssc.ssc, StorageLevel.MEMORY_AND_DISK_SER_2, brokerUrl, topics,
      userName, userPass, Option(sslOptions), client, Option(cleanSession), Option(qos),
      None, None, None)

  }

  /********** SCALA **********/

  /**
   * @param ssc           StreamingContext object
   * @param storageLevel  RDD storage level. Defaults to StorageLevel.MEMORY_AND_DISK_SER_2.
   * @param brokerUrl     Url of remote MQTT publisher
   * @param topics        Array of topic names to subscribe to
   * @param userName			 Name of the mqtt user
   * @param userPass      Password of the mqtt user
   */
  def createStream(
    ssc: StreamingContext,
    storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2,
    brokerUrl: String,
    topics: Array[String],
    userName: String,
    userPass: String): ReceiverInputDStream[MqttEvent] = {
    new MqttInputDStream(ssc, storageLevel, brokerUrl, topics, userName, userPass)
  }

  /**
   * @param ssc               StreamingContext object
   * @param storageLevel      RDD storage level. Defaults to StorageLevel.MEMORY_AND_DISK_SER_2.
   * @param brokerUrl         Url of remote MQTT publisher
   * @param topics            Array of topic names to subscribe to
   * @param userName					 Name of the mqtt user
   * @param userPass          Password of the mqtt user
 	 * @param sslOptions        SSL authentication
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
    storageLevel: StorageLevel,
    brokerUrl: String,
    topics: Array[String],
    userName: String,
    userPass: String,
    sslOptions: Option[SSLOptions],
    clientId: Option[String],
    cleanSession: Option[Boolean],
    qos: Option[Int],
    connectionTimeout: Option[Int],
    keepAliveInterval: Option[Int],
    mqttVersion: Option[Int]): ReceiverInputDStream[MqttEvent] = {
    new MqttInputDStream(ssc, storageLevel, brokerUrl, topics, userName, userPass, sslOptions, clientId,
      cleanSession, qos, connectionTimeout, keepAliveInterval, mqttVersion)
  }

}
