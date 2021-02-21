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
import javax.net.ssl.SSLContext

import okhttp3._
import okhttp3.sse._

import de.kp.works.stream.ssl._

// TODO SSL SUPPORT

class SseClient(
    serverUrl: String,
    authToken: Option[String],
    sslOptions: Option[SSLOptions] = None) {
  /*
   * This is an internal helper method to create an OkHttpClient
   * that trusts all certificates
   */
  private def createUnsafeClient:OkHttpClient = {
    
    try {

      val allTrustManagers = SslUtil.getAllTrustManagers
      val sslContext =  SSLContext.getInstance("TLS")
      
      sslContext.init(null, allTrustManagers, new java.security.SecureRandom())
      val sslSocketFactory = sslContext.getSocketFactory

      val builder = new OkHttpClient.Builder()
      
      val x509TrustManager = allTrustManagers(0).asInstanceOf[javax.net.ssl.X509TrustManager]
      builder.sslSocketFactory(sslSocketFactory, x509TrustManager)
      
      builder.build
      
    } catch {
      case t:Throwable => null
    }
  
  }
  
  private def createSafeClient:OkHttpClient = {
    
    try {
      
      val options = sslOptions.get
      
      val sslSocketFactory = options.getSslSocketFactory
      val x509TrustManager = options.getTrustManagerFactory.getTrustManagers()(0).asInstanceOf[javax.net.ssl.X509TrustManager]
      
      val builder = new OkHttpClient.Builder()
      builder.sslSocketFactory(sslSocketFactory, x509TrustManager)
      
      builder.build
      
    } catch {
      case t:Throwable => null
    }
    
  }
  
  def getHttpClient:OkHttpClient = {

    if (sslOptions.isDefined)
      createSafeClient
    
    else
      createUnsafeClient

  }
  
  def getRequest:Request = {
    /*
     * Build request with an optional authentication token
     */
    val builder = new Request.Builder()
      .url(serverUrl)

      val request = {
      if (authToken.isDefined) 
        builder
          .addHeader("Authorization", "Bearer " + authToken.get)
      else 
        builder

    }.build

    request
    
  }

}