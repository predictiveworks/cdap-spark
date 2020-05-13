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

import java.nio.charset.StandardCharsets

import com.google.gson;

import org.eclipse.paho.client.mqttv3._
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.receiver.Receiver

/**
 * Input stream that exposes subscribe messages from a Mqtt Broker.
 * Uses eclipse paho as MqttClient http://www.eclipse.org/paho/
 * 
 * @param _ssc               Spark Streaming StreamingContext
 * @param brokerUrl          Url of remote mqtt publisher
 * @param topic              topic name Array to subscribe to
 * @param storageLevel       RDD storage level.
 * @param clientId           ClientId to use for the mqtt connection
 * @param username           Username for authentication to the mqtt publisher
 * @param password           Password for authentication to the mqtt publisher
 * @param cleanSession       Sets the mqtt cleanSession parameter
 * @param qos                Quality of service to use for the topic subscriptions
 * @param connectionTimeout  Connection timeout for the mqtt connection
 * @param keepAliveInterval  Keepalive interal for the mqtt connection
 * @param mqttVersion        Version to use for the mqtt connection
 */
class MqttInputDStream(
    _ssc: StreamingContext,
    brokerUrl: String,
    topics: Array[String],
    storageLevel: StorageLevel,
    clientId: Option[String] = None,
    username: Option[String] = None,
    password: Option[String] = None,
    cleanSession: Option[Boolean] = None,
    qos: Option[Int] = None,
    connectionTimeout: Option[Int] = None,
    keepAliveInterval: Option[Int] = None,
    mqttVersion: Option[Int] = None
  ) extends ReceiverInputDStream[MqttResult](_ssc) {

  override def name: String = s"MQTT stream [$id]"

  def getReceiver(): Receiver[MqttResult] = {
    new MqttReceiver(brokerUrl, topics, storageLevel, clientId, username, password, cleanSession,
      qos, connectionTimeout, keepAliveInterval, mqttVersion)
  }
}

class MqttReceiver(
    brokerUrl: String,
    topics: Array[String],
    storageLevel: StorageLevel,
    clientId: Option[String],
    username: Option[String],
    password: Option[String],
    cleanSession: Option[Boolean],
    qos: Option[Int],
    connectionTimeout: Option[Int],
    keepAliveInterval: Option[Int],
    mqttVersion: Option[Int]
  ) extends Receiver[MqttResult](storageLevel) {

  def onStop() {

  }
  
  private def getOptions(): MqttConnectOptions = {

    /* Initialize mqtt parameters */
    val options = new MqttConnectOptions()
    if (username.isDefined && password.isDefined) {
      
      options.setUserName(username.get)
      options.setPassword(password.get.toCharArray)
    
    }
    options.setCleanSession(cleanSession.getOrElse(true))
    if (connectionTimeout.isDefined) {
      options.setConnectionTimeout(connectionTimeout.get)
    }
    
    if (keepAliveInterval.isDefined) {
      options.setKeepAliveInterval(keepAliveInterval.get)
    }
    
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

    /* Set up persistence for messages */
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
        
        val payload = new String(message.getPayload(), StandardCharsets.UTF_8)
        val result = new MqttResult(topic, payload)
        
        store(result)
        
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
