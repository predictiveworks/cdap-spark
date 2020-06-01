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

import java.nio.charset.Charset
import java.security.MessageDigest
import java.util.Date

import com.google.gson;

import org.eclipse.paho.client.mqttv3._
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence

import org.eclipse.paho.client.mqttv3.internal.security.SSLSocketFactoryFactory

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.receiver.Receiver

import de.kp.works.stream.ssl._


/**
 * MQTT v3.1 & MQTT v3.1.1
 * 
 * 
 * This input stream uses the Eclipse Paho MqttClient (http://www.eclipse.org/paho/).
 * 
 * Eclipse Paho is an umbrella project for several MQTT and MQTT-SN client implementations. 
 * This project was one of the first open source MQTT client implementations available and 
 * is actively maintained by a huge community. 
 * 
 * Paho features a Java client which is suited for embedded use, Android applications and 
 * Java applications in general. The initial code base was donated to Eclipse by IBM in 2012.
 * 
 * The Eclipse Paho Java client is rock-solid and is used by a broad range of companies from 
 * different industries around the world to connect to MQTT brokers. 
 * 
 * The synchronous/blocking API of Paho makes it easy to implement the applications MQTT logic 
 * in a clean and concise way while the asynchronous API gives the application developer full 
 * control for high-performance MQTT clients that need to handle high throughput. 
 * 
 * The Paho API is highly callback based and allows to hook in custom business logic to different 
 * events, e.g. when a message is received or when the connection to the broker was lost. 
 * 
 * Paho supports all MQTT features and a secure communication with the MQTT Broker is possible 
 * via TLS.
 * 
 * @param _ssc               Spark Streaming StreamingContext
 * @param brokerUrl          Url of remote mqtt publisher
 * @param topic              topic name Array to subscribe to
 * @param storageLevel       RDD storage level.
 * @param userName					 Name of the mqtt user
 * @param userPass           Password of the mqtt user
 * @param sslOptions         SSL authentication
 * @param clientId           ClientId to use for the mqtt connection
 * @param cleanSession       Sets the mqtt cleanSession parameter
 * @param qos                Quality of service to use for the topic subscriptions
 * @param connectionTimeout  Connection timeout for the mqtt connection
 * @param keepAliveInterval  Keepalive interal for the mqtt connection
 * @param mqttVersion        Version to use for the mqtt connection
 */
class MqttInputDStream(
    _ssc: StreamingContext,
    storageLevel: StorageLevel,
    brokerUrl: String,
    topics: Array[String],
    userName: String,
    userPass: String,
    sslOptions: Option[SSLOptions] = None,
    clientId: Option[String] = None,
    cleanSession: Option[Boolean] = None,
    qos: Option[Int] = None,
    connectionTimeout: Option[Int] = None,
    keepAliveInterval: Option[Int] = None,
    mqttVersion: Option[Int] = None
  ) extends ReceiverInputDStream[MqttEvent](_ssc) {

  override def name: String = s"MQTT stream [$id]"

  def getReceiver(): Receiver[MqttEvent] = {
    new MqttReceiver(storageLevel, brokerUrl, topics, userName, userPass, sslOptions, clientId, cleanSession,
      qos, connectionTimeout, keepAliveInterval, mqttVersion)
  }
}

class MqttReceiver(
    storageLevel: StorageLevel,
    brokerUrl: String,
    topics: Array[String],
    userName: String,
    userPass: String,
    sslOptions: Option[SSLOptions] = None,
    clientId: Option[String],
    cleanSession: Option[Boolean],
    qos: Option[Int],
    connectionTimeout: Option[Int],
    keepAliveInterval: Option[Int],
    mqttVersion: Option[Int]
  ) extends Receiver[MqttEvent](storageLevel) {

  def onStop() {

  }
  
  private def getOptions(): MqttConnectOptions = {

    /* Initialize mqtt parameters */
    val options = new MqttConnectOptions()

    /* User authentication */
    options.setUserName(userName)
    options.setPassword(userPass.toCharArray)
    
    if (sslOptions.isDefined)
      options.setSocketFactory(sslOptions.get.getSslSocketFactory)

    options.setCleanSession(cleanSession.getOrElse(true))
    
    if (connectionTimeout.isDefined) {
      options.setConnectionTimeout(connectionTimeout.get)
    }
    
    if (keepAliveInterval.isDefined) {
      options.setKeepAliveInterval(keepAliveInterval.get)
    }
    
    /*
     * Connect with MQTT 3.1 or MQTT 3.1.1
     * 
     * Depending which MQTT broker you are using, you may want to explicitely 
     * connect with a specific MQTT version.
     * 
     * By default, Paho tries to connect with MQTT 3.1.1 and falls back to 
     * MQTT 3.1 if it’s not possible to connect with 3.1.1.
     * 
     * We therefore do not specify a certain MQTT version.
     */
    
    if (mqttVersion.isDefined) {
      options.setMqttVersion(mqttVersion.get)
    }
    
    options
    
  }
  
  /*
   * A use case for this receiver, e.g. is the The Thing Stack
   * application server that exposes LoRAWAN devices via MQTT 
   */
  def onStart() {

    val UTF8 = Charset.forName("UTF-8")        
    val MD5 = MessageDigest.getInstance("MD5")

    /* 						MESSAGE PERSISTENCE
     * 
     * Since we don’t want to persist the state of pending 
     * QoS messages and the persistent session, we are just 
     * using a in-memory persistence. A file-based persistence 
     * is used by default.
     */
    val persistence = new MemoryPersistence()

    /* Initializing Mqtt Client specifying brokerUrl, clientID and MqttClientPersistance */
    val client = new MqttClient(brokerUrl, clientId.getOrElse(MqttClient.generateClientId()),
                                persistence)

    /* Initialize mqtt parameters */
    val options = getOptions()

    /* 
     * Callback automatically triggers as and when new message 
     * arrives on specified topic 
     */    
    val callback = new MqttCallback() {

      override def messageArrived(topic: String, message: MqttMessage) {

        /* Timestamp when the message arrives */
        val timestamp = new Date().getTime
        val seconds = timestamp / 1000
        
        val payload = message.getPayload()
        if (payload == null) {
          
          /* Do nothing */
          
        } else {

          val qos = message.getQos
          
          val duplicate = message.isDuplicate()
          val retained = message.isRetained()
          
          /* Serialize plain byte message */
  			    val json = new String(payload, UTF8);
  
  			    /* Extract metadata from topic and
  			     * serialized payload
  			     */
          val serialized = Seq(topic, json).mkString("|")
          val digest = MD5.digest(serialized.getBytes).toString
         
  			    val tokens = topic.split("\\/").toList
  			  
  			    val context = MD5.digest(tokens.init.mkString("|").getBytes).toString
  			    val dimension = tokens.last
         
          val result = new MqttEvent(timestamp, seconds, topic, qos, duplicate, retained, payload, digest, json, context, dimension)
          store(result)
          
        }
        
      }

      override def deliveryComplete(token: IMqttDeliveryToken) {
      }

      override def connectionLost(cause: Throwable) {
        restart("Connection lost ", cause)
      }
      
    }

    /* 
     * Set up callback for MqttClient. This needs to happen before
     * connecting or subscribing, otherwise messages may be lost
     */
    client.setCallback(callback)

    /* Connect to MqttBroker */
    client.connect(options)

    /* Subscribe to Mqtt topics */

    val quality = topics.map(topic => qos.getOrElse(1))    
    client.subscribe(topics, quality)

  }

}
