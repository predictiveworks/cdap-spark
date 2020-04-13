package de.kp.works.ditto
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
import org.eclipse.ditto.client.configuration._
import org.eclipse.ditto.client.messaging._

import org.eclipse.ditto.model.base.json.JsonSchemaVersion
import com.neovisionaries.ws.client.WebSocket

object DittoHelper {
  
  def getMessagingProvider(properties: java.util.Properties): MessagingProvider = {
    
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
    
    val authProvider = getAuthProvider(proxyConfiguration, properties)
    MessagingProviders.webSocket(builder.build(), authProvider)
    
  }
  
  def getAuthProvider(proxyConf:ProxyConfiguration, properties: java.util.Properties):AuthenticationProvider[WebSocket] = {
    
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