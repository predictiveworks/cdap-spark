package org.apache.spark.streaming.ws
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
import java.util.{Optional, Properties}

import org.apache.spark.storage.StorageLevel

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.receiver.Receiver

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

class DittoInputDStream(
    ssc_ : StreamingContext,
    properties: Properties,
    storageLevel: StorageLevel) extends ReceiverInputDStream[String](ssc_) {
  
  override def name: String = s"Web socket stream [$id]"
  
  def getReceiver(): Receiver[String] = {
    new DittoReceiver(properties, storageLevel)
  }

}

class DittoReceiver(
    properties: Properties,
    storageLevel: StorageLevel) extends Receiver[String](storageLevel) {

  private var client:DittoClient = _
  
  def onStop() {

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

  def onStart() {    
     
    /*
     * Build Ditto websocket client
     */
    val messagingProvider:MessagingProvider = getMessagingProvider
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
            /*
             * Transform message into Option[String] and 
             * send for message handler
             */
            val payload = message.getPayload
            if (payload.isPresent()) {
             
              store(payload.get)
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
  
  private def getMessagingProvider(): MessagingProvider = {
    
    val builder = WebSocketMessagingConfiguration.newBuilder()
    /* See Bosch IoT examples */
    builder.jsonSchemaVersion(JsonSchemaVersion.V_2)
    
    val endpoint = properties.getProperty(DittoUtils.DITTO_ENDPOINT)
    builder.endpoint(endpoint)

    val proxyConfiguration: ProxyConfiguration = {
      
      if (
          properties.containsKey(DittoUtils.DITTO_PROXY_HOST) && 
          properties.containsKey(DittoUtils.DITTO_PROXY_PORT)) {
        
        val host = properties.getProperty(DittoUtils.DITTO_PROXY_HOST)
        val port = properties.getProperty(DittoUtils.DITTO_PROXY_PORT)
        
        if (host == null || port == null) null
        else {
          
          val proxyConf = ProxyConfiguration.newBuilder()
            .proxyHost(host)
            .proxyPort(Integer.parseInt(port))
            .build
            
          proxyConf
          
        }
        
      } else null
      
    }
    
    if (
        properties.containsKey(DittoUtils.DITTO_TRUSTSTORE_LOCATION) && 
        properties.contains(DittoUtils.DITTO_TRUSTSTORE_PASSWORD)) {
             
        val location = properties.getProperty(DittoUtils.DITTO_TRUSTSTORE_LOCATION)
        val password = properties.getProperty(DittoUtils.DITTO_TRUSTSTORE_PASSWORD)
        
        if (location != null && password != null) {
          
          val trustStoreConf = TrustStoreConfiguration.newBuilder()
            .location(new java.net.URL(location))
            .password(password)
            .build
            
          builder.trustStoreConfiguration(trustStoreConf)
            
        }
    }
    
    val authProvider = getAuthProvider(proxyConfiguration)
    MessagingProviders.webSocket(builder.build(), authProvider)
    
  }
  
  private def getAuthProvider(proxyConf:ProxyConfiguration):AuthenticationProvider[WebSocket] = {
    
    if (properties.containsKey(DittoUtils.DITTO_USER) && properties.containsKey(DittoUtils.DITTO_PASS)) {
      
      /** BASIC AUTHENTICATION **/
      
      val user = properties.getProperty(DittoUtils.DITTO_USER)
      val pass = properties.getProperty(DittoUtils.DITTO_PASS)
      
      if (user == null || pass == null) {
        throw new IllegalArgumentException("Basic authentication requires username & password.")
      }
      
      val basicAuthConf = BasicAuthenticationConfiguration.newBuilder()
        .username(user)
        .password(pass)
 
      if (proxyConf != null) basicAuthConf.proxyConfiguration(proxyConf)  

      AuthenticationProviders.basic(basicAuthConf.build)
      
    } else {
      
      /** AUTH2 AUTHENTICATION **/
      
      try {
        
        val scopes = {
          
          val tokens = properties.getProperty(DittoUtils.DITTO_OAUTH_SCOPES).split(",")
          
          val s = new java.util.ArrayList[String]()
          tokens.foreach(t => s.add(t.trim))
          
          s
          
        }
        
        val oAuthConf = ClientCredentialsAuthenticationConfiguration.newBuilder()
          .clientId(DittoUtils.DITTO_OAUTH_CLIENT_ID)
          .clientSecret(DittoUtils.DITTO_OAUTH_CLIENT_SECRET)
          .scopes(scopes)
          .tokenEndpoint(DittoUtils.DITTO_OAUTH_TOKEN_ENDPOINT)
 
        if (proxyConf != null) oAuthConf.proxyConfiguration(proxyConf)  
        AuthenticationProviders.clientCredentials(oAuthConf.build)
        
      } catch {
        case e: Exception => throw new IllegalArgumentException("Missing parameters for OAuth authentication.")
      }

    }
     
  }
  
}