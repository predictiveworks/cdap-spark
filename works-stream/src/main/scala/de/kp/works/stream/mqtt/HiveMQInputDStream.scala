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

import java.util.{Date, UUID}
import java.nio.charset.Charset

import java.security.MessageDigest
import java.security.Security

import com.hivemq.client.mqtt._
import com.hivemq.client.mqtt.datatypes._

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

import org.bouncycastle.jce.provider.BouncyCastleProvider

import org.slf4j.{Logger, LoggerFactory}

import de.kp.works.stream.ssl._

import scala.collection.JavaConversions._

class HiveMQInputDStream(
    _ssc: StreamingContext,
    storageLevel: StorageLevel,
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
    mqttVersion: Option[Int] = None    
    )  extends ReceiverInputDStream[MqttResult](_ssc) {
 
    override def name: String = s"HiveMQ stream [$id]"
 
    def getReceiver(): Receiver[MqttResult] = {
      new HiveMQReceiver(storageLevel, mqttTopic, mqttHost, mqttPort, mqttUser, mqttPass, mqttSsl, mqttQoS, mqttVersion)
    }
    
}

class HiveMQReceiver(
    storageLevel: StorageLevel,
    mqttTopic: String,
    mqttHost: String,
    mqttPort: Int,
    mqttUser: String,
    mqttPass: String,
    mqttSsl: Option[SSLOptions],
    mqttQoS: Option[Int] = None,    
    mqttVersion: Option[Int] = None    
    ) extends Receiver[MqttResult](storageLevel) {

    	private final val LOG = LoggerFactory.getLogger(classOf[HiveMQReceiver])

    private val UTF8 = Charset.forName("UTF-8")        
    private val MD5 = MessageDigest.getInstance("MD5")

    /* 
     * Set up callback for MqttClient. This needs to happen before
     * connecting or subscribing, otherwise messages may be lost
     */
    private val mqtt3Callback = new java.util.function.Consumer[Mqtt3Publish] {
      
      def accept(publish: Mqtt3Publish):Unit = {

        /* Timestamp when the message arrives */
        val timestamp = new Date().getTime
        val seconds = timestamp / 1000
        
        val payload = publish.getPayloadAsBytes
        
        /* Parse plain byte message */
			  val json = new String(payload, UTF8);

        val serialized = Seq(mqttTopic, payload).mkString("|")
        val digest = MD5.digest(serialized.getBytes).toString
       
			  val tokens = mqttTopic.split("\\/").toList
			  
			  val context = MD5.digest(tokens.init.mkString("|").getBytes).toString
			  val dimension = tokens.last
          
        val result = new MqttResult(timestamp, seconds, mqttTopic, payload, digest, json, context, dimension)
        store(result)
        
      }
      
    }
    	
    private val mqtt5Callback = new java.util.function.Consumer[Mqtt5Publish] {
      
      def accept(publish: Mqtt5Publish):Unit = {

        /* Timestamp when the message arrives */
        val timestamp = new Date().getTime
        val seconds = timestamp / 1000
        
        val payload = publish.getPayloadAsBytes
        
        /* Parse plain byte message */
			  val json = new String(payload, UTF8);

        val serialized = Seq(mqttTopic, payload).mkString("|")
        val digest = MD5.digest(serialized.getBytes).toString
       
			  val tokens = mqttTopic.split("\\/").toList
			  
			  val context = MD5.digest(tokens.init.mkString("|").getBytes).toString
			  val dimension = tokens.last
          
        val result = new MqttResult(timestamp, seconds, mqttTopic, payload, digest, json, context, dimension)
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
        
        val simpleAuth = Mqtt3SimpleAuth
          .builder()
          .username(mqttUser)
          .password(mqttPass.getBytes(Charset.forName("UTF-8")))
          .build()
          
        builder.simpleAuth(simpleAuth)
       
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

        val simpleAuth = Mqtt5SimpleAuth
          .builder()
          .username(mqttUser)
          .password(mqttPass.getBytes(Charset.forName("UTF-8")))
          .build()
          
        builder.simpleAuth(simpleAuth)
       
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
                .qos(getMqttQoS)
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