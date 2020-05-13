package de.kp.works.stream.creds
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

import javax.net.ssl.SSLSocketFactory

import java.security._
import java.security.cert.X509Certificate

class X509Credentials(  
  val caCert:X509Certificate,
  val cert:X509Certificate,
  val privateKey:PrivateKey, 
  val password:Option[String] = None) extends Credentials {
  
  def getSSLSocketFactory:SSLSocketFactory = {
    
    /* Delegate to [CertificateUtil] */
    CertificateUtil.getSSLSocketFactory(caCert,cert,privateKey,password)

  }
  
}