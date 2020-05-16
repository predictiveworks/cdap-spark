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

object HiveMQUtils {
  
  /********** JAVA **********/

  
  /********** SCALA **********/

  /**
   * @param ssc           StreamingContext object
   * @param storageLevel  RDD storage level. Defaults to StorageLevel.MEMORY_AND_DISK_SER_2.
   * @param mqttHost      Host of the MQTT broker
   * @param mqttPort      Port of the MQTT broker
   * @param topics        Array of topic names to subscribe to
   */
  def createStream(
      ssc: StreamingContext,
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2,
      mqttTopic: String,
      mqttHost: String,
      mqttPort: Int,
      topics: Array[String]
    ): ReceiverInputDStream[MqttResult] = {
    new HiveMQInputDStream(ssc, storageLevel, mqttTopic, mqttHost, mqttPort)
  }
  
}