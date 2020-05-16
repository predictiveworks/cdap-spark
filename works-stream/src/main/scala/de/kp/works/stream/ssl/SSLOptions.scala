package de.kp.works.stream.ssl
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

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

class SSLOptions(
  
    /* KEY STORE */
  val keystoreFile:         String,
  val keystoreType:         String,
  val keystorePassword:     String,
  val keystoreAlgorithm:    String,
  
  /* TRUST STORE */
  val verifyHttps:          Boolean,
  
	val truststoreFile:       Option[String] = None, 
	val truststoreType:       Option[String] = None, 
	val truststorePassword:   Option[String] = None,  
	val  truststoreAlgorithm: Option[String] = None,
  
  val cipherSuites: Option[Array[String]]) {

  def getCipherSuites: List[String] = {

    if (cipherSuites.isDefined)
      cipherSuites.get.toList

    else
      null

  }

  def getHostnameVerifier: HostnameVerifier = {

    /* No hostname verification */
    NoopHostnameVerifier.INSTANCE

  }

  def getKeyManagerFactory: KeyManagerFactory = {

    try {

      SslUtil.getKeyManagerFactory(keystoreFile, keystoreType, 
          keystorePassword, keystoreAlgorithm)

    } catch {

      case t: Throwable =>
        /* Do nothing */
        null
    }

  }

  def getTrustManagerFactory: TrustManagerFactory = {
    null
  }
}