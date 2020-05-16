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

import java.util.UUID
import java.nio.charset.Charset

import com.hivemq.client.mqtt._
import com.hivemq.client.mqtt.datatypes.MqttTopicFilter

import com.hivemq.client.mqtt.mqtt3._
import com.hivemq.client.mqtt.mqtt3.message.auth._
import com.hivemq.client.mqtt.mqtt3.message.connect.connack.Mqtt3ConnAck;
import com.hivemq.client.mqtt.mqtt3.message.publish.Mqtt3Publish
import com.hivemq.client.mqtt.mqtt3.message.subscribe.suback.Mqtt3SubAck;

import com.hivemq.client.mqtt.mqtt5._
import com.hivemq.client.mqtt.mqtt5.message.auth._
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAck;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish
import com.hivemq.client.mqtt.mqtt5.message.subscribe.suback.Mqtt5SubAck;

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.receiver.Receiver

import org.slf4j.{Logger, LoggerFactory}

import de.kp.works.stream.creds._
import de.kp.works.stream.ssl._

import scala.collection.JavaConversions._

class HiveMQInputDStream(
    _ssc: StreamingContext,
    storageLevel: StorageLevel,
    mqttTopic: String,
    mqttHost: String,
    mqttPort: Int,  
    /*
     * User authentication	
     */
    mqttCreds: Option[Credentials] = None,
    /*
     * Transport security
     */
    mqttSSL: Option[SSLOptions] = None,
    mqttVersion: Option[Int] = None    
    )  extends ReceiverInputDStream[MqttResult](_ssc) {
 
    override def name: String = s"HiveMQ stream [$id]"
 
    def getReceiver(): Receiver[MqttResult] = {
      new HiveMQReceiver(storageLevel, mqttTopic, mqttHost, mqttPort, mqttCreds, mqttSSL, mqttVersion)
    }
    
}

class HiveMQReceiver(
    storageLevel: StorageLevel,
    mqttTopic: String,
    mqttHost: String,
    mqttPort: Int,
    /*
     * User authentication	
     */
    mqttCreds: Option[Credentials],
    /*
     * Transport security
     */
    mqttSSL: Option[SSLOptions],
    mqttVersion: Option[Int] = None    
    ) extends Receiver[MqttResult](storageLevel) {

    	private final val LOG = LoggerFactory.getLogger(classOf[HiveMQReceiver])

    /* 
     * Set up callback for MqttClient. This needs to happen before
     * connecting or subscribing, otherwise messages may be lost
     */
    private val mqtt3Callback = new java.util.function.Consumer[Mqtt3Publish] {
      
      def accept(publish: Mqtt3Publish):Unit = {
        
        val payload = publish.getPayloadAsBytes
        val result = new MqttResult(mqttTopic, payload)
        
        store(result)
        
      }
      
    }
    	
    private val mqtt5Callback = new java.util.function.Consumer[Mqtt5Publish] {
      
      def accept(publish: Mqtt5Publish):Unit = {
        
        val payload = publish.getPayloadAsBytes
        val result = new MqttResult(mqttTopic, payload)
        
        store(result)
        
      }
      
    }

    private def listenToMqtt3: Unit = {
      
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
           
        if (mqttCreds.isDefined) {
      
          val creds = mqttCreds.get
          if (creds.isInstanceOf[BasicCredentials]) {
        
            val basicCreds = creds.asInstanceOf[BasicCredentials]
            val simpleAuth = Mqtt3SimpleAuth
              .builder()
              .username(basicCreds.username)
              .password(basicCreds.password.getBytes(Charset.forName("UTF-8")))
              .build()
              
            builder.simpleAuth(simpleAuth)

          }
      
        }
       
        /***** CONNECT & SUBSCRIBE *****/
          
        val client = builder.buildAsync()
        /*
         * Define the subscription callback, and, log the 
         * respective results
         */                
        val onSubscription = new java.util.function.BiConsumer[Mqtt3SubAck, Throwable] {
          
          def accept(connAck: Mqtt3SubAck, throwable: Throwable):Unit = {  
            
            /* Error handling */
            if (throwable != null) {
              LOG.error("Subscription failed: " + throwable.getLocalizedMessage);

            } else {
              LOG.debug("Subscription successful");
              
            }
          
          }

        }
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
              
              client
                .subscribeWith()
                .topicFilter(mqttTopic)
                .callback(mqtt3Callback)
                .send()
                .whenComplete(onSubscription)

            }
          }
        
        }
         
        client
         .connectWith()
         .send()
         .whenComplete(onConnection)

    	}
    
    private def listenToMqtt5:Unit = {
      
        val identifier = UUID.randomUUID().toString()
        
        val builder = Mqtt5Client.builder()
          .identifier(identifier)
          .serverHost(mqttHost)
          .serverPort(mqttPort)
          
        /* Transport layer security */
        
        val sslConfig = getMqttSslConfig
        if (sslConfig != null) builder.sslConfig(sslConfig)
          
        /* Application layer security */
        
        if (mqttCreds.isDefined) {
      
          val creds = mqttCreds.get
          if (creds.isInstanceOf[BasicCredentials]) {
        
            val basicCreds = creds.asInstanceOf[BasicCredentials]
            val simpleAuth = Mqtt5SimpleAuth
              .builder()
              .username(basicCreds.username)
              .password(basicCreds.password.getBytes(Charset.forName("UTF-8")))
              .build()
              
            builder.simpleAuth(simpleAuth)

          }
          
        }
       
        /***** CONNECT & SUBSCRIBE *****/
          
        val client = builder.buildAsync()
        /*
         * Define the subscription callback, and, log the 
         * respective results
         */                
        val onSubscription = new java.util.function.BiConsumer[Mqtt5SubAck, Throwable] {
          
          def accept(connAck: Mqtt5SubAck, throwable: Throwable):Unit = {  
            
            /* Error handling */
            if (throwable != null) {
              LOG.error("Subscription failed: " + throwable.getLocalizedMessage);

            } else {
              LOG.debug("Subscription successful");
              
            }
          
          }

        }
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
              
              client
                .subscribeWith()
                .topicFilter(mqttTopic)
                .callback(mqtt5Callback)
                .send()
                .whenComplete(onSubscription)

            }
          }
        
        }
         
        client
         .connectWith()
         .send()
         .whenComplete(onConnection)

    }
    
    /* Transport layer security */
    private def getMqttSslConfig: MqttClientSslConfig = {
  
        if (mqttSSL.isDefined) {
          
          val sslOptions = mqttSSL.get
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
    
    def onStop() {}
  
    def onStart() {

      /*
       * HiveMQ supports MQTT version 5 as well as version 3;
       * default is version 3
       */
      val version = mqttVersion.getOrElse(3)
      if (version == 3)
          listenToMqtt3
          
      else
          listenToMqtt5
      
    }

}