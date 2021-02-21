package de.kp.works.stream.sse
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

import org.apache.spark.storage.StorageLevel

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream._

import org.apache.spark.streaming.receiver.Receiver

import okhttp3._
import okhttp3.sse._

import de.kp.works.stream.ssl._

class SseInputDStream(
    _ssc: StreamingContext,
    storageLevel: StorageLevel,
    serverUrl: String,
    authToken: Option[String] = None,
    sslOptions: Option[SSLOptions] = None) extends ReceiverInputDStream[SseEvent](_ssc) {

  override def name: String = s"Server Sent Events (SSE) stream [$id]"
  
  def getReceiver(): Receiver[SseEvent] = {
    new SseReceiver(storageLevel, serverUrl, authToken, sslOptions)
  }
  
}

class SseReceiver(
    storageLevel: StorageLevel,
    serverUrl: String,
    authToken: Option[String],
    sslOptions: Option[SSLOptions] = None) extends Receiver[SseEvent](storageLevel) {
  
  def onStop() {
  }

  def onStart() {
    
    val sseClient = new SseClient(serverUrl, authToken, sslOptions)

    val request = sseClient.getRequest
    val httpClient = sseClient.getHttpClient
    
    /** SSE **/
    
    val factory = EventSources.createFactory(httpClient)
    val listener = new EventSourceListener() {
      
      override def onOpen(eventSource:EventSource, response:Response):Unit = {
        
      }
      
      override def onEvent(eventSource:EventSource, id:String, `type`:String, data:String):Unit = {
        val result = new SseEvent(id, `type`, data)
        store(result)        
      }
      
      override def onClosed(eventSource:EventSource) {
        
      }
      
      override def onFailure(eventSource:EventSource, t:Throwable, response:Response) {
        /* Restart the receiver in case of an error */
        restart("Connection lost ", t)        
      }
      
    }

    factory.newEventSource(request, listener)

  }
}