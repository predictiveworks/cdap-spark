package de.kp.works.stream.pubsub
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

import com.google.api.client.auth.oauth2.Credential
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory

import com.google.api.services.pubsub.Pubsub.Builder
import com.google.api.services.pubsub.model.Subscription
import com.google.api.services.pubsub.model.{AcknowledgeRequest, PubsubMessage, PullRequest}

import com.google.cloud.hadoop.util.RetryHttpInitializer

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.receiver.Receiver

import scala.util.control.NonFatal
import scala.collection.JavaConverters._

class PubSubInputDStream(
    _ssc: StreamingContext,
    storageLevel: StorageLevel,
    project: String,
    topic: Option[String],
    subscription: String,
    credential: Credential,
    autoAcknowledge: Boolean  
) extends ReceiverInputDStream[PubSubResult](_ssc) {

  override def getReceiver(): Receiver[PubSubResult] = {
    new PubSubReceiver(storageLevel, project, topic, subscription, credential, autoAcknowledge)
  }
}

class PubSubReceiver(
    storageLevel: StorageLevel,
    project: String,
    topic: Option[String],
    subscription: String,
    credential: Credential,
    autoAcknowledge: Boolean
  ) extends Receiver[PubSubResult](storageLevel) {
  
    val APP_NAME = "sparkstreaming-pubsub-receiver"
  
    /* 100ms */
    val INIT_BACKOFF = 100
    /* 10s */
    val MAX_BACKOFF = 10 * 1000

    val MAX_MESSAGE = 1000

    val projectFullName: String = s"projects/$project"
    val subscriptionFullName: String = s"$projectFullName/subscriptions/$subscription"
  
    lazy val client = new Builder(
        ConnectionUtils.transport,
        ConnectionUtils.jacksonFactory,
        new RetryHttpInitializer(credential, APP_NAME)
     )
    .setApplicationName(APP_NAME)
    .build()

    def receive(): Unit = {
    
      val pullRequest = new PullRequest()
        .setMaxMessages(MAX_MESSAGE)
        .setReturnImmediately(false)
      
      
      var backoff = INIT_BACKOFF
      
      while (!isStopped()) {

        try {
        
          val pullResponse = client
              .projects()
              .subscriptions()
              .pull(subscriptionFullName, pullRequest)
              .execute()
          
          val receivedMessages = pullResponse.getReceivedMessages
          if (receivedMessages != null) {
          
            val messages = receivedMessages.asScala
            val output = messages
              .filter(m => m.isEmpty() == false)
              .map(m => {

                val message = m.getMessage
                /*
                 * Convert to a HashMap because com.google.api.client.util.ArrayMap 
                 * is not serializable.
                 */
                val attributes = new java.util.HashMap[String,String]()
                attributes.putAll(message.getAttributes)
                
                PubSubResult(
                    ackId = m.getAckId, 
                    publishTime = message.getPublishTime, 
                    attributes = attributes, 
                    data = message.decodeData()
                )

              })

            if (output.isEmpty == false)
              store(output.iterator)
            
            if (autoAcknowledge) {
            
                val ackRequest = new AcknowledgeRequest()
                ackRequest.setAckIds(messages.map(x => x.getAckId).asJava)
              
                client
                  .projects()
                  .subscriptions()
                  .acknowledge(subscriptionFullName, ackRequest)
                  .execute()
            }

          }
          
          backoff = INIT_BACKOFF
          
        } catch {
          case e: GoogleJsonResponseException =>
            if (ConnectionUtils.retryable(e.getDetails.getCode)) {

              Thread.sleep(backoff)
              backoff = Math.min(backoff * 2, MAX_BACKOFF)

            } else {
              reportError("Failed to pull messages", e)
            
            }
          case NonFatal(e) => reportError("Failed to pull messages", e)
        }
      }
      
    }
    
    override def onStart(): Unit = {
      
      topic match {
        case Some(t) => {
        
          val sub: Subscription = new Subscription
          sub.setTopic(s"$projectFullName/topics/$t")
          
          try {
          
            client
              .projects()
              .subscriptions()
              .create(subscriptionFullName, sub)
              .execute()
            
          } catch {
            
            case e: GoogleJsonResponseException => {
              if (e.getDetails.getCode == ConnectionUtils.ALREADY_EXISTS) {
                
                /* Ignore subscription already exists exception. */
                
              } else {
                reportError("Failed to create subscription", e)
              }

            }
            case NonFatal(e) =>
              reportError("Failed to create subscription", e)
            }
          
        }
        case None => /* Do nothing */
      }

      new Thread() {

        override def run() {
          receive()
        }
        
      }.start()

    }
    
    override def onStop(): Unit = {}
    
}

object ConnectionUtils {

    val transport = GoogleNetHttpTransport.newTrustedTransport()
    val jacksonFactory = JacksonFactory.getDefaultInstance

    /* 
     * The topic or subscription already exists. This is an error 
     * on creation operations.
     */
    val ALREADY_EXISTS = 409
    /*
     * Client can retry with these response status
     */
    val RESOURCE_EXHAUSTED = 429

    val CANCELLED = 499
    val INTERNAL = 500

    val UNAVAILABLE = 503
    val DEADLINE_EXCEEDED = 504

    def retryable(status: Int): Boolean = {

      status match {
        case RESOURCE_EXHAUSTED | CANCELLED | INTERNAL | UNAVAILABLE | DEADLINE_EXCEEDED => true
        case _ => false
      }
      
    }
}