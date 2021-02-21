package de.kp.works.stream.sse
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
import org.apache.spark.streaming.api.java.{ JavaDStream, JavaReceiverInputDStream, JavaStreamingContext }
import org.apache.spark.streaming.dstream.ReceiverInputDStream

import de.kp.works.stream.ssl._

object SseUtils {

  /********** JAVA **********/

  /**
   * Storage level of the data will be the default
   * StorageLevel.MEMORY_AND_DISK_SER_2.
   *
   * @param jssc      JavaStreamingContext object
   * @param serverUrl  Url of remote server
   * 
   */
  def createStream(
    jssc: JavaStreamingContext,
    serverUrl: String): JavaReceiverInputDStream[SseEvent] = {

    implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[String]]
    createStream(jssc.ssc, StorageLevel.MEMORY_AND_DISK_SER_2,serverUrl, None, None)

  }

  /**
   * @param jssc         JavaStreamingContext object
   * @param storageLevel RDD storage level.
   * @param serverUrl    Url of remote server
   * @param authToken    Authentication token
   */
  def createStream(
    jssc: JavaStreamingContext,
    storageLevel: StorageLevel,
    serverUrl: String,
    authToken: String): JavaReceiverInputDStream[SseEvent] = {

    implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[String]]
    createStream(jssc.ssc, storageLevel, serverUrl, Option(authToken), None)

  }

  /**
   * @param jssc              JavaStreamingContext object
   * @param storageLevel      RDD storage level.
   * @param serverUrl         Url of remote server
   * @param authToken         Authentication token
 	 * @param sslOptions        SSL authentication
   */
  def createStream(
    jssc: JavaStreamingContext,
    storageLevel: StorageLevel,
    serverUrl: String,
    authToken: String,
    sslOptions: SSLOptions): JavaReceiverInputDStream[SseEvent] = {

    implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[String]]
    createStream(jssc.ssc, storageLevel, serverUrl, Option(authToken), Option(sslOptions))

  }

  /********** SCALA **********/

  /**
   * @param ssc               StreamingContext object
   * @param storageLevel      RDD storage level. Defaults to StorageLevel.MEMORY_AND_DISK_SER_2.
   * @param serverUrl         Url of remote server
   * @param authToken         Authentication Token
 	 * @param sslOptions        SSL authentication
   */
  def createStream(
    ssc: StreamingContext,
    storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2,
    serverUrl: String,
    authToken: Option[String],
    sslOptions: Option[SSLOptions]): ReceiverInputDStream[SseEvent] = {
    new SseInputDStream(ssc, storageLevel, serverUrl, authToken, sslOptions)
  }

}

class SseUtils {
  
}