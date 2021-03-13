package de.kp.works.ts
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

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.catalyst.expressions.TimeWindow

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object TSUtils {
  
  /*
   * A helper method to define a window specification
   * to leverage Apache Spark' row_number function
   */
  def rowNumberSpec(timeCol:String): WindowSpec = {
    Window.partitionBy().orderBy(timeCol)
    
  }

  def rowNumberSpec(groupCol:String, timeCol:String): WindowSpec = {
    Window.partitionBy(groupCol).orderBy(timeCol)
    
  }

  /*
   * A helper method to add a 'cyle' column to a time series based
   * on a lag parameter (periodicity of seasonality); this method
   * is used by the STL algorithm
   */
  def addCycleColumn(timeset:Dataset[_], timeCol:String, lag:Int):Dataset[_] = {

    val modulo = modulo_udf(lag)
    
    timeset
    /*
     * Add row number to dataset using Apache Spark's row_number function
     */
    .withColumn("_rn", row_number().over(rowNumberSpec(timeCol)))
    /*
     * Add cyclic column and remove row number _rn
     */
    .withColumn("cycle", modulo(col("_rn"))).drop("_rn")

  }
  
  def addCycleColumn(timeset:Dataset[_], groupCol:String, timeCol:String, lag:Int):Dataset[_] = {

    val modulo = modulo_udf(lag)
    
    timeset
    /*
     * Add row number to dataset using Apache Spark's row_number function
     */
    .withColumn("_rn", row_number().over(rowNumberSpec(groupCol, timeCol)))
    /*
     * Add cyclic column and remove row number _rn
     */
    .withColumn("cycle", modulo(col("_rn"))).drop("_rn")

  }
  
  def modulo_udf(lag:Int) = udf {
    number:Long => number % lag
  }
  
  def validateDuration(windowDuration:String) {
    
    /*
     * A Catalyst TimeWindow is used to parse and validate
     * a window duration expression
     */
    val expr = col("*").expr
    TimeWindow(expr, windowDuration, windowDuration, "0 second")
    
  }
  def windowDuration2Long(windowDuration:String): Long = {
    
    /*
     * A Catalyst TimeWindow is used to (a) parse and validate
     * a window duration expression and second transform the
     * expression into a Long
     */
    val expr = col("*").expr
    
     val timeWindow = TimeWindow(expr, windowDuration, windowDuration, "0 second")
     timeWindow.windowDuration
 
  }

}
