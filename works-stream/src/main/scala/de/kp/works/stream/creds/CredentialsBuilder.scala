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

import java.security._
import java.security.cert.X509Certificate

object CredentialsBuilder {
  
    def createBasic(username:String, password:String):BasicCredentials = {
      new BasicCredentials(username, password)
    }
    
    def createPEMX509(username:String, password:String, caCrtFile:String, crtFile:String, 
        keyFile:String, keyPass:Option[String] = None):PEMX509Credentials = {

      new PEMX509Credentials(username, password, caCrtFile, crtFile, keyFile, keyPass)  
    
    }

    def createX509(username:String, password:String, caCert: X509Certificate, cert:X509Certificate, 
        privateKey:PrivateKey, keyPass:Option[String] = None):X509Credentials = {
      
      new X509Credentials(username, password, caCert, cert, privateKey, keyPass)
    
    }
    
}