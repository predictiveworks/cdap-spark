package org.apache.spark.stream.ws
/*
 * Copyright (c) 2019 Dr. Krusche & Partner PartG. All rights reserved.
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
import org.apache.spark.storage.StorageLevel

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.receiver.Receiver

class WSInputDStream(
    @transient ssc_ : StreamingContext,
    storageLevel: StorageLevel    
  ) extends ReceiverInputDStream[String](ssc_) {
  
  override def name: String = s"Web socket stream [$id]"
  
  def getReceiver(): Receiver[String] = {
    new WSReceiver(storageLevel)
  }

}

class WSReceiver(
    storageLevel: StorageLevel    
  ) extends Receiver[String](storageLevel) {

  def onStop() {
  }

  def onStart() {    
  }

}