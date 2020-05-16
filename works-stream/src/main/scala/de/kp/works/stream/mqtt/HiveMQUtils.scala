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
import de.kp.works.stream.ssl._

object HiveMQUtils {
  
  /********** JAVA **********/
  
  /**
   * Storage level of the data will be the default 
   * StorageLevel.MEMORY_AND_DISK_SER_2.
   *
   * @param jssc      		JavaStreamingContext object
   * @param mqttTopic     MQTT topic to listen to
   * @param mqttHost      Host of the MQTT broker
   * @param mqttPort      Port of the MQTT broker
   */
  def createStream(
      jssc: JavaStreamingContext,
      mqttTopic: String,
      mqttHost: String,
      mqttPort: Int
    ): JavaReceiverInputDStream[MqttResult] = {
    
    createStream(jssc, StorageLevel.MEMORY_AND_DISK_SER_2, mqttTopic,mqttHost, mqttPort)
    
  }
  /**
   * @param jssc      		JavaStreamingContext object
   * @param storageLevel  RDD storage level.
   * @param mqttTopic     MQTT topic to listen to
   * @param mqttHost      Host of the MQTT broker
   * @param mqttPort      Port of the MQTT broker
   */
  def createStream(
      jssc: JavaStreamingContext,
      storageLevel: StorageLevel,
      mqttTopic: String,
      mqttHost: String,
      mqttPort: Int
    ): JavaReceiverInputDStream[MqttResult] = {
    
    implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[String]]
    createStream(jssc.ssc, storageLevel, mqttTopic,mqttHost, mqttPort)
    
  }
  
  /**
   * @param jssc      		JavaStreamingContext object
   * @param storageLevel  RDD storage level.
   * @param mqttTopic     MQTT topic to listen to
   * @param mqttHost      Host of the MQTT broker
   * @param mqttPort      Port of the MQTT broker
   * @param mqttVersion   MQTT version (either 3 or 5)
   */
  def createStream(
      jssc: JavaStreamingContext,
      storageLevel: StorageLevel,
      mqttTopic: String,
      mqttHost: String,
      mqttPort: Int,
      mqttVersion: Int      
    ): JavaReceiverInputDStream[MqttResult] = {
    
    implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[String]]
    createStream(jssc.ssc, storageLevel, mqttTopic,mqttHost, mqttPort, None, None, None, Option(mqttVersion))
    
  }
  
  /**
   * @param jssc      		JavaStreamingContext object
   * @param storageLevel  RDD storage level.
   * @param mqttTopic     MQTT topic to listen to
   * @param mqttHost      Host of the MQTT broker
   * @param mqttPort      Port of the MQTT broker
   * @param mqttQoS       Quality of service to use for the topic subscription
   * @param mqttVersion   MQTT version (either 3 or 5)
   */
  def createStream(
      jssc: JavaStreamingContext,
      storageLevel: StorageLevel,
      mqttTopic: String,
      mqttHost: String,
      mqttPort: Int,
      mqttQoS: Int,      
      mqttVersion: Int      
    ): JavaReceiverInputDStream[MqttResult] = {
    
    implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[String]]
    createStream(jssc.ssc, storageLevel, mqttTopic,mqttHost, mqttPort, None, None, Option(mqttQoS), Option(mqttVersion))
    
  }
  
  /**
   * @param jssc      		JavaStreamingContext object
   * @param storageLevel  RDD storage level.
   * @param mqttTopic     MQTT topic to listen to
   * @param mqttHost      Host of the MQTT broker
   * @param mqttPort      Port of the MQTT broker
   * @param mqttCreds     Application security (username & password)
   * @param mqttQoS       Quality of service to use for the topic subscription
   * @param mqttVersion   MQTT version (either 3 or 5)
   */
  def createStream(
      jssc: JavaStreamingContext,
      storageLevel: StorageLevel,
      mqttTopic: String,
      mqttHost: String,
      mqttPort: Int,
      mqttCreds: Credentials,
      mqttQoS: Int,      
      mqttVersion: Int      
    ): JavaReceiverInputDStream[MqttResult] = {
    
    implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[String]]
    createStream(jssc.ssc, storageLevel, mqttTopic,mqttHost, mqttPort, Option(mqttCreds), None, Option(mqttQoS), Option(mqttVersion))
    
  }
  
  /**
   * @param jssc      		JavaStreamingContext object
   * @param storageLevel  RDD storage level.
   * @param mqttTopic     MQTT topic to listen to
   * @param mqttHost      Host of the MQTT broker
   * @param mqttPort      Port of the MQTT broker
   * @param mqttCreds     Application security (username & password)
   * @param mqttSsl       Transport security
   * @param mqttQoS       Quality of service to use for the topic subscription
   * @param mqttVersion   MQTT version (either 3 or 5)
   */
  def createStream(
      jssc: JavaStreamingContext,
      storageLevel: StorageLevel,
      mqttTopic: String,
      mqttHost: String,
      mqttPort: Int,
      mqttCreds: Credentials,
      mqttSsl: SSLOptions,
      mqttQoS: Int,      
      mqttVersion: Int      
    ): JavaReceiverInputDStream[MqttResult] = {
    
    implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[String]]
    createStream(jssc.ssc, storageLevel, mqttTopic,mqttHost, mqttPort, Option(mqttCreds), Option(mqttSsl), Option(mqttQoS), Option(mqttVersion))
    
  }

  
  /********** SCALA **********/

  /**
   * @param ssc           StreamingContext object
   * @param storageLevel  RDD storage level. Defaults to StorageLevel.MEMORY_AND_DISK_SER_2.
   * @param mqttTopic     MQTT topic to listen to
   * @param mqttHost      Host of the MQTT broker
   * @param mqttPort      Port of the MQTT broker
   * @param mqttCreds     Application security (username & password)
   * @param mqttSsl       Transport security
   * @param mqttQoS       Quality of service to use for the topic subscription
   * @param mqttVersion   MQTT version (either 3 or 5)
   */
  def createStream(
      ssc: StreamingContext,
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2,
      mqttTopic: String,
      mqttHost: String,
      mqttPort: Int,
      mqttCreds: Option[Credentials] = None,
      mqttSsl: Option[SSLOptions] = None,
      mqttQoS: Option[Int] = None,
      mqttVersion: Option[Int] = None          
    ): ReceiverInputDStream[MqttResult] = {
    new HiveMQInputDStream(ssc, storageLevel, mqttTopic, mqttHost, mqttPort, mqttCreds, mqttSsl, mqttQoS, mqttVersion)
  }
  
}
