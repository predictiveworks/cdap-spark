package de.kp.works.stream.x509
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

import java.io._
import java.nio.file._

import java.security._
import java.security.spec._
import java.security.cert.X509Certificate
import javax.net.ssl._

import org.bouncycastle.jce.provider.BouncyCastleProvider

import org.bouncycastle.openssl._
import org.bouncycastle.openssl.jcajce._

object CertificateUtil {
  
  val CA_CERT_ALIAS = "caCert-cert"
  val CERT_ALIAS = "cert"
  
  val PRIVATE_KEY_ALIAS = "private-key"
  val TLS_VERSION = "TLSv1.2"
 
	def getSSLSocketFactory():SSLSocketFactory = {
    	
    val sslContext = SSLContext.getInstance(TLS_VERSION)
    sslContext.init(null, null, null)
    	
    sslContext.getSocketFactory()
	  
	}
 
  def getSSLSocketFactory(caCert:X509Certificate,cert:X509Certificate,privateKey:PrivateKey,password:Option[String] = None):SSLSocketFactory = {

	  Security.addProvider(new BouncyCastleProvider())
	  /*
	   * STEP #1: Build TrustManagerFactory and establish
	   * certificate trust chain
	   */
	  val caCertAlias = CA_CERT_ALIAS
		val trustManagerFactory = getTrustManagerFactory(caCertAlias, caCert)
		/*
		 * STEP #2: Build KeyManagerFactory
		 */
	  val certAlias = CERT_ALIAS
	  val keyAlias  = PRIVATE_KEY_ALIAS
	  
		/* Initialize key manager factory */
		val keyManagerFactory = getKeyManagerFactory(certAlias, cert, keyAlias, privateKey, password)

		/* Set up client certificate for connection over TLS */
		val sslContext = SSLContext.getInstance(TLS_VERSION)
		sslContext.init(keyManagerFactory.getKeyManagers, trustManagerFactory.getTrustManagers, null)

		/* Set connect options to use the TLS enabled socket factory */
		sslContext.getSocketFactory

  }

  /**
   * NOTE: This method has been validated with IBM IoT client library
   */
  def getSSLSocketFactoryFromFiles(caCrtFile:String,crtFile:String,keyFile:String,password:Option[String] = None):SSLSocketFactory = {

	  Security.addProvider(new BouncyCastleProvider())
	  /*
	   * STEP #1: Retrieve CA certificate and build
	   * TrustManagerFactory
	   */
	  val caCert = getX509CertFromPEM(caCrtFile)
	  val caCertAlias = CA_CERT_ALIAS

		/* Establish certificate trust chain */
		val trustManagerFactory = getTrustManagerFactory(caCertAlias, caCert)
		/*
		 * STEP #2: Retrieve client certificate	 and build
		 * KeyManagerFactory
		 */
	  val cert = CertificateUtil.getX509CertFromPEM(crtFile)
	  val certAlias = CERT_ALIAS
	  
	  val privateKey:PrivateKey = getPrivateKeyFromPEM(keyFile,password)
	  val keyAlias  = PRIVATE_KEY_ALIAS
	  
		/* Initialize key manager factory */
		val keyManagerFactory = getKeyManagerFactory(certAlias, cert, keyAlias, privateKey, password)

		// Set up client certificate for connection over TLS 1.2
		val sslContext = SSLContext.getInstance(TLS_VERSION)
		sslContext.init(keyManagerFactory.getKeyManagers, trustManagerFactory.getTrustManagers, null)

		/* Set connect options to use the TLS enabled socket factory */
		sslContext.getSocketFactory
    
  }
  /**
   * NOTE: This method has been validated with IBM IoT client library
   */
  def getX509CertFromPEM(crtFile:String):X509Certificate = {
    /*
     * Since Java cannot read PEM formatted certificates, this method 
     * is using bouncy castle (http://www.bouncycastle.org/) to load 
     * the necessary files.
     * 
     * IMPORTANT: Bouncycastle Provider must be added before this
     * method can be called
     * 
     */
    val bytes = Files.readAllBytes(Paths.get(crtFile))
    val bais = new ByteArrayInputStream(bytes)
    
    val reader = new PEMParser(new InputStreamReader(bais))
    val cert = reader.readObject.asInstanceOf[X509Certificate]
    
		reader.close()
		cert    

  }

  /**
   * NOTE: This method has been validated with IBM IoT client library
   */
  def getPrivateKeyFromPEM(keyFile:String,password:Option[String]):PrivateKey = {
    
    val bytes = Files.readAllBytes(Paths.get(keyFile))
    val bais = new ByteArrayInputStream(bytes)
    
    val reader = new PEMParser(new InputStreamReader(bais))
    val keyObject = reader.readObject
    
    reader.close
    
    val keyPair = if (keyObject.isInstanceOf[PEMEncryptedKeyPair]) {

      if (password.isDefined == false)
        throw new Exception("[ERROR] Reading private key from file without password is not supported.")
        
      val passwordArray = password.get.toCharArray
      val provider = new JcePEMDecryptorProviderBuilder().build(passwordArray)
      
      keyObject.asInstanceOf[PEMEncryptedKeyPair].decryptKeyPair(provider)
      
    } else {
      /*
       * NOTE: This use case is compliant with IBM Watson
       * IoT platform
       */
      reader.readObject.asInstanceOf[PEMKeyPair]
    }
		  
    val factory = KeyFactory.getInstance("RSA", "BC")
    val keySpec = new PKCS8EncodedKeySpec(keyPair.getPrivateKeyInfo().getEncoded())

    factory.generatePrivate(keySpec)
   
  }
  /**
   * NOTE: IBM Watson IoT platform leverages JKS as key store type
   */
  def createKeystore():KeyStore = {
      /*
       * Create a default (JKS) keystore without any password. 
       * Method load(null, null) indicates to create a new one 
       */
			val keystore = KeyStore.getInstance(KeyStore.getDefaultType)    
		  keystore.load(null, null)
    
		  keystore
		  
  }
  
  def getKeyManagerFactory(certAlias:String,cert:X509Certificate,keyAlias:String,key:PrivateKey,password:Option[String]):KeyManagerFactory = {
    
    val ks = createKeystore
    
    /* Add client certificate to key store, the client certificate
     * alias is 'certificate' (see IBM Watson IoT platform) 
     */
    ks.setCertificateEntry(certAlias, cert)

    /* Add private key to keystore and distinguish
     * between use case with and without password
     */
    val passwordArray = 
      if (password.isDefined) password.get.toCharArray else new Array[Char](0)
    
		ks.setKeyEntry(keyAlias,key,passwordArray,Array(cert))

		/* Initialize key manager from the key store; note, the default 
		 * algorithm also supported by IBM Watson IoT platform is PKIX
		 */
		val keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
		keyManagerFactory.init(ks, passwordArray)

		keyManagerFactory
		
  }
  /**
   * NOTE: This method has been validated with IBM IoT client library
   */
  def getTrustManagerFactory(alias:String,cert:X509Certificate):TrustManagerFactory = {
    
    val ks = createKeystore
    /*
     * Add CA certificate to keystore; note, the CA certificate alias
     * is set to 'ca-certificate' (see IBM Watson IoT platform)
     */
		ks.setCertificateEntry(alias, cert)

		/* Establish certificate trust chain; note, the default algorithm
		 * also supported by IBM Watson IoT platform is PKIX
		 */
		val trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
		trustManagerFactory.init(ks)
		
		trustManagerFactory

  }
  
}