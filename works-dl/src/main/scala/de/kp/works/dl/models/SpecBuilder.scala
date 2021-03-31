package de.kp.works.dl.models
/*
 * Copyright (c) 2019 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
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

import com.typesafe.config.{Config, ConfigList, ConfigFactory, ConfigObject}

import org.apache.log4j.Logger

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

trait SpecBuilder {
       
  val logger = Logger.getLogger(getClass)
   
  /*
   * A helper method to transform a String specification
   * of a Keras Model into Config
   */
  def spec2Config(rawSpec:String):Config = {
    
    try {

      val cleanSpec = rawSpec.trim
        .replaceAll("\n", "")
        .replaceAll("\r", "")
        .replaceAll("\\s+", " ")
  
      ConfigFactory.parseString(cleanSpec)
      
    } catch {
      case t:Throwable => {
        throw new Exception("The Keras model specification cannot be parsed. Consider to change the specification.")
      }
    }
      
  }

  def getAsBoolean(config:Config, name:String, default:Boolean) = {
    
    try {
      config.getBoolean(name)

    } catch {
      case t:Throwable => default
    }

  }
  
  def getAsDouble(config:Config, name:String, default:Double) = {
    
    try {
      config.getDouble(name)

    } catch {
      case t:Throwable => default
    }

  }
  
  def getAsDoubleArray(config:ConfigList):Array[Double] = {
      
    val values = ArrayBuffer.empty[Double]
    config.foreach(v => 
      values += v.render.toDouble
    )
    
    values.toArray

  }
  
  def getAsInt(config:Config, name:String, default:Int) = {
    
    try {
      config.getInt(name)

    } catch {
      case t:Throwable => default
    }

  }
  
  def getAsIntArray(config:ConfigList):Array[Int] = {
      
    val values = ArrayBuffer.empty[Int]
    config.foreach(v => 
      values += v.render.toInt
    )
    
    values.toArray

  }

  def getAsString(config:Config, name:String, default:String) = {
    
    try {
      config.getString(name)

    } catch {
      case t:Throwable => default
    }

  }
/*
 * 
      val params = layer.getConfig("params")
      /* 
       * Int tuple of length 2 corresponding to the downscale 
       * vertically and horizontally. Default is (2, 2), which 
       * will halve the image in each dimension.
       */
      val poolSize = config2IntArray(params.getList("poolSize"))
      MaxPooling2D(poolSize = (poolSize(0), poolSize(1)))

 * 
 */
}