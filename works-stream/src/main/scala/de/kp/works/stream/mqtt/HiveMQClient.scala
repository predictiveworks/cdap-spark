package de.kp.works.stream.mqtt
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
import java.util.{UUID}
import java.nio.charset.Charset

import java.security.Security

import com.hivemq.client.mqtt._
import com.hivemq.client.mqtt.datatypes._

import com.hivemq.client.mqtt.mqtt3._
import com.hivemq.client.mqtt.mqtt3.message.auth._
import com.hivemq.client.mqtt.mqtt3.message.connect.connack.Mqtt3ConnAck

import com.hivemq.client.mqtt.mqtt5._
import com.hivemq.client.mqtt.mqtt5.message.auth._
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAck

import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.slf4j.{Logger, LoggerFactory}

import de.kp.works.stream.ssl._
import scala.collection.JavaConversions._

object HiveMQClient {
  
  def build(
    mqttTopic: String,
    mqttHost: String,
    mqttPort: Int,  
    mqttUser: String,
    mqttPass: String): HiveMQClient = {
    
    build(mqttTopic, mqttHost, mqttPort, mqttUser, mqttPass, None, None, None)
    
  }
  
  def build(
    mqttTopic: String,
    mqttHost: String,
    mqttPort: Int,  
    mqttUser: String,
    mqttPass: String,
    /*
     * Transport security
     */
    mqttSsl: SSLOptions): HiveMQClient = {
    
    new HiveMQClient(mqttTopic, mqttHost, mqttPort, mqttUser, mqttPass, Option(mqttSsl), None, None)
    
  }
  
  def build(
    mqttTopic: String,
    mqttHost: String,
    mqttPort: Int,  
    mqttUser: String,
    mqttPass: String,
    /*
     * Transport security
     */
    mqttSsl: SSLOptions,
    mqttQoS: Int): HiveMQClient = {
    
    build(mqttTopic, mqttHost, mqttPort, mqttUser, mqttPass, Option(mqttSsl), Option(mqttQoS), None)
    
  }
  
  def build(
    mqttTopic: String,
    mqttHost: String,
    mqttPort: Int,  
    mqttUser: String,
    mqttPass: String,
    /*
     * Transport security
     */
    mqttSsl: Option[SSLOptions] = None,
    mqttQoS: Option[Int] = None,    
    mqttVersion: Option[Int] = None): HiveMQClient = {
    
    new HiveMQClient(mqttTopic, mqttHost, mqttPort, mqttUser, mqttPass, mqttSsl, mqttQoS, mqttVersion)
    
  }
  
}

class HiveMQClient(
    mqttTopic: String,
    mqttHost: String,
    mqttPort: Int,  
    mqttUser: String,
    mqttPass: String,
    /*
     * Transport security
     */
    mqttSsl: Option[SSLOptions] = None,
    mqttQoS: Option[Int] = None,    
    mqttVersion: Option[Int] = None) {

    	private final val LOG = LoggerFactory.getLogger(classOf[HiveMQClient])

    	private var connected:Boolean = false
    	
    private var mqtt3Client: Mqtt3AsyncClient = null
    private var mqtt5Client: Mqtt5AsyncClient = null

    def connect: Unit = {
      /*
       * HiveMQ supports MQTT version 5 as well as version 3;
       * default is version 3
       */
      val version = mqttVersion.getOrElse(3)
      if (version == 3)
          connectToMqtt3
          
      else
          connectToMqtt5
  
    	}

    def publish: Unit = {
      /*
       * HiveMQ supports MQTT version 5 as well as version 3;
       * default is version 3
       */
      val version = mqttVersion.getOrElse(3)
      if (version == 3)
          publishToMqtt3
          
      else
          publishToMqtt5
  
    	}
    
    	/***** MQTT 3 *****/
    	
    	private def connectToMqtt3: Unit = {
      
        /***** BUILD CLIENT *****/
      
        val identifier = UUID.randomUUID().toString()
        
        val builder = Mqtt3Client.builder()
          .identifier(identifier)
          .serverHost(mqttHost)
          .serverPort(mqttPort)
          
        /* Transport layer security */
        
        val sslConfig = getMqttSslConfig
        if (sslConfig != null) builder.sslConfig(sslConfig)
          
        /* Application layer security */
        
        val simpleAuth = Mqtt3SimpleAuth
          .builder()
          .username(mqttUser)
          .password(mqttPass.getBytes(Charset.forName("UTF-8")))
          .build()
          
        builder.simpleAuth(simpleAuth)
       
        /***** CONNECT *****/
          
        mqtt3Client = builder.buildAsync()
        /*
         * Define the connection callback, and, in case of a 
         * successful connection, continue to subscribe to
         * an MQTT topic
         */        
        val onConnection = new java.util.function.BiConsumer[Mqtt3ConnAck, Throwable] {
          
          def accept(connAck: Mqtt3ConnAck, throwable: Throwable):Unit = {
            
            /* Error handling */
            if (throwable != null) {
              /*
               * In case of an error, the respective message is log,
               * but streaming is continued
               */
              LOG.error(throwable.getLocalizedMessage)
              
            } else {
              
              LOG.info("Connecting to HiveMQ Broker successfull")
              connected = true

            }
          }
        
        }
        
        mqtt3Client
         .connectWith()
         .send()
         .whenComplete(onConnection)

    	}
    
    	private def publishToMqtt3:Unit = {
    	  
    	  if (connected == false || mqtt3Client == null) {
    	    throw new Exception("No connection to HiveMQ server established")
    	  }
    	  
    	}
    	
    	/***** MQTT 5 *****/
    	
    private def connectToMqtt5: Unit = {
      
        /***** BUILD CLIENT *****/
      
        val identifier = UUID.randomUUID().toString()
        
        val builder = Mqtt5Client.builder()
          .identifier(identifier)
          .serverHost(mqttHost)
          .serverPort(mqttPort)
          
        /* Transport layer security */
        
        val sslConfig = getMqttSslConfig
        if (sslConfig != null) builder.sslConfig(sslConfig)
          
        /* Application layer security */
        
        val simpleAuth = Mqtt5SimpleAuth
          .builder()
          .username(mqttUser)
          .password(mqttPass.getBytes(Charset.forName("UTF-8")))
          .build()
          
        builder.simpleAuth(simpleAuth)
       
        /***** CONNECT *****/
          
        mqtt5Client = builder.buildAsync()
        /*
         * Define the connection callback, and, in case of a 
         * successful connection, continue to subscribe to
         * an MQTT topic
         */        
        val onConnection = new java.util.function.BiConsumer[Mqtt5ConnAck, Throwable] {
          
          def accept(connAck: Mqtt5ConnAck, throwable: Throwable):Unit = {
            
            /* Error handling */
            if (throwable != null) {
              /*
               * In case of an error, the respective message is log,
               * but streaming is continued
               */
              LOG.error(throwable.getLocalizedMessage)
              
            } else {
              
              LOG.info("Connecting to HiveMQ Broker successfull")
              connected = true

            }
          }
        
        }
        
        mqtt5Client
         .connectWith()
         .send()
         .whenComplete(onConnection)

    	}
    
    	private def publishToMqtt5:Unit = {
    	  
    	  if (connected == false || mqtt5Client == null) {
    	    throw new Exception("No connection to HiveMQ server established")
    	  }
    	  
    	}
  
    private def getMqttQoS: MqttQos = {
      
      var qos = mqttQoS.getOrElse(1);
      qos match {
        case 0 => {
          /*
           * QoS for at most once delivery according to the 
           * capabilities of the underlying network.
           */
          MqttQos.AT_MOST_ONCE
        }
        case 1 => {
          /*
           * QoS for ensuring at least once delivery.
           */
          MqttQos.AT_LEAST_ONCE
        }
        case 2 => {
          /*
           * QoS for ensuring exactly once delivery.
           */
          MqttQos.EXACTLY_ONCE
        }
        case _ => throw new RuntimeException(s"Quality of Service '${qos}' is not supported.")
      }
      
    }
    
    /* Transport layer security */
    private def getMqttSslConfig: MqttClientSslConfig = {
  
        if (mqttSsl.isDefined) {

		      Security.addProvider(new BouncyCastleProvider())
          
          val sslOptions = mqttSsl.get
          val builder = MqttClientSslConfig.builder()
          
          /* CipherSuites */
          val cipherSuites = sslOptions.getCipherSuites
          if (cipherSuites != null)
              builder.cipherSuites(cipherSuites)
          
          /* Hostname verifier */
          builder.hostnameVerifier(sslOptions.getHostnameVerifier)
          
          /* Key manager factory */
          val keyManagerFactory = sslOptions.getKeyManagerFactory
          if (keyManagerFactory != null)
              builder.keyManagerFactory(keyManagerFactory)
          
          /* Trust manager factory */
          val trustManagerFactory = sslOptions.getTrustManagerFactory
          if (trustManagerFactory != null)
              builder.trustManagerFactory(trustManagerFactory)
      
              
          builder.build()
          
      } else null
    
    }

}