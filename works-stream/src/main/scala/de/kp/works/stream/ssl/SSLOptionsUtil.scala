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

import java.security._
import java.security.cert.X509Certificate

object SSLOptionsUtil {

/***** KEY & TRUST STORES *****/

  def buildStoreOptions(
    keystoreFile: String,
    keystoreType: String,
    keystorePassword: String,
    keystoreAlgorithm: String):SSLOptions = {

    new SSLOptions(
      keystoreFile = Option(keystoreFile),
      keystoreType = Option(keystoreType),
      keystorePassword = Option(keystorePassword),
      keystoreAlgorithm = Option(keystoreAlgorithm))
  }

  def buildStoreOptions(
    keystoreFile: String,
    keystoreType: String,
    keystorePassword: String,
    keystoreAlgorithm: String,
    cipherSuites: List[String]):SSLOptions = {

    new SSLOptions(
      keystoreFile = Option(keystoreFile),
      keystoreType = Option(keystoreType),
      keystorePassword = Option(keystorePassword),
      keystoreAlgorithm = Option(keystoreAlgorithm),
      cipherSuites = Option(cipherSuites.toArray))
  }

  def buildStoreOptions(
    keystoreFile: String,
    keystoreType: String,
    keystorePassword: String,
    keystoreAlgorithm: String,
    truststoreFile: String,
    truststoreType: String,
    truststorePassword: String,
    truststoreAlgorithm: String):SSLOptions = {

    new SSLOptions(
      keystoreFile = Option(keystoreFile),
      keystoreType = Option(keystoreType),
      keystorePassword = Option(keystorePassword),
      keystoreAlgorithm = Option(keystoreAlgorithm),
      truststoreFile = Option(truststoreFile),
      truststoreType = Option(truststoreType),
      truststorePassword = Option(truststorePassword),
      truststoreAlgorithm = Option(truststoreAlgorithm))
  }

  def buildStoreOptions(
    keystoreFile: String,
    keystoreType: String,
    keystorePassword: String,
    keystoreAlgorithm: String,
    truststoreFile: String,
    truststoreType: String,
    truststorePassword: String,
    truststoreAlgorithm: String,
    cipherSuites: List[String]):SSLOptions = {

    new SSLOptions(
      keystoreFile = Option(keystoreFile),
      keystoreType = Option(keystoreType),
      keystorePassword = Option(keystorePassword),
      keystoreAlgorithm = Option(keystoreAlgorithm),
      truststoreFile = Option(truststoreFile),
      truststoreType = Option(truststoreType),
      truststorePassword = Option(truststorePassword),
      truststoreAlgorithm = Option(truststoreAlgorithm),
      cipherSuites = Option(cipherSuites.toArray))
  }

/***** CERTIFICATES *****/

  def buildCertOptions(
    caCert: X509Certificate,
    cert: X509Certificate,
    privateKey: PrivateKey,
    privateKeyPass: String):SSLOptions = {

    new SSLOptions(
      caCert = Option(caCert),
      cert = Option(cert),
      privateKey = Option(privateKey),
      privateKeyPass = Option(privateKeyPass))

  }

  def buildCertOptions(
    caCert: X509Certificate,
    cert: X509Certificate,
    privateKey: PrivateKey,
    privateKeyPass: String,
    cipherSuites: List[String]):SSLOptions = {

    new SSLOptions(
      caCert = Option(caCert),
      cert = Option(cert),
      privateKey = Option(privateKey),
      privateKeyPass = Option(privateKeyPass),
      cipherSuites = Option(cipherSuites.toArray))

  }

/***** CERTIFICATE FILES *****/

  def buildCertFileOptions(
    caCertFile: String,
    certFile: String,
    privateKeyFile: String,
    privateKeyFilePass: String):SSLOptions = {

    new SSLOptions(
      caCertFile = Option(caCertFile),
      certFile = Option(certFile),
      privateKeyFile = Option(privateKeyFile),
      privateKeyFilePass = Option(privateKeyFilePass))

  }

  def buildCertFileOptions(
    caCertFile: String,
    certFile: String,
    privateKeyFile: String,
    privateKeyFilePass: String,
    cipherSuites: List[String]):SSLOptions = {

    new SSLOptions(
      caCertFile = Option(caCertFile),
      certFile = Option(certFile),
      privateKeyFile = Option(privateKeyFile),
      privateKeyFilePass = Option(privateKeyFilePass),
      cipherSuites = Option(cipherSuites.toArray))

  }

}