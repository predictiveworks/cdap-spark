package org.apache.ignite.stream.ws
/*
 * Copyright (c) 2019 Dr. Krusche & Partner PartG. All rights reserved.
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

import org.apache.ignite.stream.StreamAdapter
import org.apache.ignite.IgniteLogger

import org.apache.ignite.internal.util.{GridArgumentCheck => A}

import org.eclipse.ditto.client.{DittoClient, DittoClients}
import org.eclipse.ditto.client.changes._
import org.eclipse.ditto.client.configuration._
import org.eclipse.ditto.client.live.messages.RepliableMessage
import org.eclipse.ditto.client.messaging._

import org.eclipse.ditto.model.base.common.HttpStatusCode;
import org.eclipse.ditto.model.base.json.JsonSchemaVersion
import org.eclipse.ditto.model.things._

import com.google.gson.Gson
import com.neovisionaries.ws.client.WebSocket

import de.kp.works.ditto._

class DittoStreamer[K,V](properties:java.util.Properties) extends StreamAdapter[String, K, V] {streamer =>

  private var client:DittoClient = _
  private var log: IgniteLogger = _
  
  /** Start streamer  */

  def start():Unit = {
    
    A.notNull(getStreamer(), "streamer")
    A.notNull(getIgnite(), "ignite")
    
    A.ensure(!(getSingleTupleExtractor() == null && getMultipleTupleExtractor() == null), 
        "tuple extractor missing")
    A.ensure(getSingleTupleExtractor() == null || getMultipleTupleExtractor() == null,
        "cannot provide both single and multiple tuple extractor")
     
    log = getIgnite().log()
    
    /*
     * Build Ditto websocket client
     */
    val messagingProvider:MessagingProvider = DittoHelper.getMessagingProvider(properties)
    client = DittoClients.newInstance(messagingProvider)
    
    /*
     * This Ditto websocket client subscribes to two protocol commands:
     * 
     * - PROTOCOL_CMD_START_SEND_EVENTS   :: "START-SEND-EVENTS"
     * - PROTOCOL_CMD_START_SEND_MESSAGES :: "START-SEND-MESSAGES"
     * 
     * Subscription to events is based on Ditto's twin implementation 
     * and refers to the TwinImpl.CONSUME_TWIN_EVENTS_HANDLER which
     * is initiated in the twin's doStartConsumption method
     * 
     * Subscription to events is based on Ditto's live implementation
     * and refers to the LiveImpl.CONSUME_LIVE_MESSAGES_HANDLER which
     * is initiated in the live's doStartConsumption method
     * 
     */
    client.twin().startConsumption().get() // EVENTS
    client.live().startConsumption().get() // MESSAGES
     
    registerForTwinEvents()
    registerForLiveMessages()
   
  }

  private def registerForTwinEvents() {
    
    val twin = client.twin()
    /*
     * Check whether a certain thing identifier is provided to 
     * restrict events to a certain thing
     */
    val thingId = if (properties.containsKey(DittoUtils.DITTO_THING_ID)) {
      ThingId.of(properties.getProperty(DittoUtils.DITTO_THING_ID))

    } else null
    /*
     * Check whether a certain feature identifier is provided to 
     * restrict events to a certain feature
     */
    val featureId = if (properties.containsKey(DittoUtils.DITTO_FEATURE_ID)) {
      properties.getProperty(DittoUtils.DITTO_FEATURE_ID)

    } else null
      
    /***** THING CHANGES *****/
    
    if (properties.containsKey(DittoUtils.DITTO_THING_CHANGES)) {
      
      val flag = properties.getProperty(DittoUtils.DITTO_THING_CHANGES)
      if (flag == "true") {

        val consumer = new java.util.function.Consumer[ThingChange] {
          override def accept(change:ThingChange):Unit = {
            
            val gson = DittoGson.thing2Gson(change)
            if (gson != null) store(gson)

          }
        }
        
        val handler = DittoUtils.DITTO_THING_CHANGES_HANDLER
        
        if (thingId == null) {

          /* Register for changes of all things */
          twin.registerForThingChanges(handler, consumer) 
         
        } else {

          /* Register for changes of thing with thingId */
          twin.forId(thingId).registerForThingChanges(handler, consumer) 
          
        }
        
      }
      
    }
      
    /***** FEATURES CHANGES *****/
    
    if (properties.containsKey(DittoUtils.DITTO_FEATURES_CHANGES)) {
      
      val flag = properties.getProperty(DittoUtils.DITTO_FEATURES_CHANGES)
      if (flag == "true") {
        
        val consumer = new java.util.function.Consumer[FeaturesChange] {
          override def accept(change:FeaturesChange):Unit = {
                
            val gson = DittoGson.features2Gson(change)
            store(gson)
              
          }
        }
        
        val handler = DittoUtils.DITTO_FEATURES_CHANGES_HANDLER
        
        if (thingId == null) {
          /* 
           * Register feature set changes of all things, as we currently 
           * do not support the provisioning of a certain thing 
           */
          twin.registerForFeaturesChanges(handler, consumer)
          
        } else {
          /* 
           * Register feature set changes of all things, as we currently 
           * do not support the provisioning of a certain thing 
           */
          twin.forId(thingId).registerForFeaturesChanges(handler, consumer)
          
        }
      }
      
    }
      
    /***** FEATURE CHANGES *****/
    
    if (properties.containsKey(DittoUtils.DITTO_FEATURE_CHANGES)) {
      /*
       * Register for all feature changes of all things
       */
      val flag = properties.getProperty(DittoUtils.DITTO_FEATURE_CHANGES)
      if (flag == "true") {
        
        val consumer = new java.util.function.Consumer[FeatureChange] {
          override def accept(change:FeatureChange):Unit = {                
                    
            val gson = DittoGson.feature2Gson(change)
            store(new Gson().toJson(gson))   
                  
          }         
        }
        
        val handler = DittoUtils.DITTO_FEATURE_CHANGES_HANDLER
        
        if (thingId != null) {
          
          if (featureId != null) {
            twin.registerForFeatureChanges(handler, featureId, consumer)

          } else
            twin.registerForFeatureChanges(handler, consumer)
          
        } else {
          
          if (featureId != null)
            twin.forId(thingId).registerForFeatureChanges(handler, featureId, consumer)
          
          else 
            twin.forId(thingId).registerForFeatureChanges(handler, consumer)
          
        }
       
      }
      
    }
    
  }
  
  private def registerForLiveMessages() {

    val live = client.live()
    /*
     * Check whether a certain thing identifier is provided to 
     * restrict events to a certain thing
     */
    val thingId = if (properties.containsKey(DittoUtils.DITTO_THING_ID)) {
      ThingId.of(properties.getProperty(DittoUtils.DITTO_THING_ID))

    } else null
        
    if (properties.containsKey(DittoUtils.DITTO_LIVE_MESSAGES)) {
      
      val flag = properties.getProperty(DittoUtils.DITTO_LIVE_MESSAGES)
      if (flag == "true") {
        
        val consumer = new java.util.function.Consumer[RepliableMessage[String, Any]] {
          override def accept(message:RepliableMessage[String, Any]) {

            val gson = DittoGson.message2Gson(message)
            if (gson != null) {
            
              store(gson)
              message.reply().statusCode(HttpStatusCode.OK).send()
             
            } else {
              message.reply().statusCode(HttpStatusCode.NO_CONTENT).send()

            }
            
          }
        }
        
        val handler = DittoUtils.DITTO_LIVE_MESSAGES
        
        if (thingId != null) {
          live.forId(thingId).registerForMessage(handler, "*", classOf[String], consumer)
          
        } else {
          live.registerForMessage(handler, "*", classOf[String], consumer)
          
        }
        
      }

    }

  }

  private def store(message: String): Unit = {
    this.addMessage(message)
  }
  
  def stop():Unit = {

    if (client != null) {
      
      /** CHANGE EVENTS **/
      
      client.twin().suspendConsumption()

      if (properties.containsKey(DittoUtils.DITTO_THING_CHANGES)) {
      
        val flag = properties.getProperty(DittoUtils.DITTO_THING_CHANGES)
        if (flag == "true") client.twin().deregister(DittoUtils.DITTO_THING_CHANGES_HANDLER)
      
      }
      if (properties.containsKey(DittoUtils.DITTO_FEATURES_CHANGES)) {
      
        val flag = properties.getProperty(DittoUtils.DITTO_FEATURES_CHANGES)
        if (flag == "true") client.twin().deregister(DittoUtils.DITTO_FEATURES_CHANGES_HANDLER)
      
      }
      if (properties.containsKey(DittoUtils.DITTO_FEATURE_CHANGES)) {
      
        val flag = properties.getProperty(DittoUtils.DITTO_FEATURE_CHANGES)
        if (flag == "true") client.twin().deregister(DittoUtils.DITTO_FEATURE_CHANGES_HANDLER)
      
      }
       
      /** LIVE MESSAGES **/
      
      client.live().suspendConsumption()
        
      if (properties.containsKey(DittoUtils.DITTO_LIVE_MESSAGES)) {
      
        val flag = properties.getProperty(DittoUtils.DITTO_LIVE_MESSAGES)
        if (flag == "true") client.live().deregister(DittoUtils.DITTO_LIVE_MESSAGES_HANDLER)
      
      }
      
      client.destroy()
      
    }
    
  }

}