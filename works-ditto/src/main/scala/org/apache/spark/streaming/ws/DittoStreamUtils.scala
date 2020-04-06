package org.apache.spark.streaming.ws
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
import java.util.Properties;

import org.apache.spark.storage.StorageLevel

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream

import org.apache.spark.streaming.api.java.{JavaInputDStream, JavaStreamingContext}

object DittoStreamUtils {
  
   def createDirectStream(
       jssc: JavaStreamingContext,
       properties: Properties,
       storageLevel: StorageLevel): JavaInputDStream[String] = {
     
     new JavaInputDStream(createDirectStream(jssc.ssc, properties, storageLevel))
  
   }

   private def createDirectStream(
       ssc: StreamingContext, 
       properties: Properties,
       storageLevel: StorageLevel): InputDStream[String] =  {
   
     new DittoInputDStream(ssc, properties, storageLevel)
     
   }
   
}

    
    
    
