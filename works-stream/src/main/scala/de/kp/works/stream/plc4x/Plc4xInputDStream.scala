package de.kp.works.stream.plc4x
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
import java.util.concurrent.TimeUnit

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.receiver.Receiver

import org.slf4j.{Logger, LoggerFactory}

class Plc4xInputDStream {
  
}

class Plc4xReceiver(
    storageLevel: StorageLevel,
    interval: Long
    ) extends Receiver[Plc4xResult](storageLevel) {

    	private final val LOG = LoggerFactory.getLogger(classOf[Plc4xReceiver])
    
    def onStop() {}
  
    def onStart() {
      
        new Thread() {
          override def run():Unit = {

            while (!isStopped()) {
              
              try {
                
                // TODO Connect to Plc4x device
                
              } catch {
                case t:Throwable =>
              }

              try {
                TimeUnit.SECONDS.sleep(interval);
                
              } catch {
                case t:Throwable => 
                  throw new RuntimeException(t)
              }
            }
          }

          override def interrupt(): Unit = {
            super.interrupt();
          }
          
        }.start();
      
    }

}