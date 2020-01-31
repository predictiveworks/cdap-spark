package com.suning.spark.ts
/*
 * Copyright (c) 2016 Suning R&D. All rights reserved.
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
 */

import com.suning.spark.util.{Identifiable, Load, Model}

import org.apache.spark.sql._
import org.apache.spark.sql.types._

abstract class TSModel(override val uid: String, inputCol: String, timeCol: String)
  extends Model {
  def this(inputCol: String, timeCol: String) =
    this(Identifiable.randomUID("TSModel"), inputCol, timeCol)

  /*
   * __KUP__ Forecast result has been changed into a dataframe that
   * contains future timestamp and associated predicted value
   */
  def forecast(df: DataFrame, numAhead: Int): DataFrame

  def forecastResult(dataset:Dataset[Row], values:Seq[Double], numAhead:Int):DataFrame = {
    
    val session = dataset.sparkSession
    /*
     * Assign timestamps to the forecasted values; to this 
     * end, we leverage the last numAhead timestamps
     */
    val n = numAhead + 1
    val times = dataset.select(timeCol).limit(n).collect()
    /*
     * Sorted timestamps in descending order, i.e. the most 
     * recent one is first
     */
    val timeType = dataset.schema(timeCol).dataType
    /*
     * The forecast result is a dataframe with two columns,
     * one contains the future timestamp with a data type
     * equal to the incoming one, and, second the predicted
     * values as Double
     */
    val schema = new StructType(Array(
        StructField(timeCol, timeType, false), StructField(inputCol, DoubleType, false)))
    
    timeType match {
      case DateType => {

        val recent = times(0).getDate(0).getTime
        /*
         * We shift the most recent past time intervals 
         * into the near future retaining their sequence 
         * in the past
         */
        val intervals = times.zip(times.tail).map(pair => {
          pair._1.getDate(0).getTime - pair._2.getDate(0).getTime      
        }).reverse
        /*
         * Before assigning the recent value to the individual
         * intervals, we have to scan (or sum up) previous values
         */
        val futures = intervals
          .scan(0L) ( _ + _ ).tail
          .map(intval => {
            val ts = recent + intval
            new java.sql.Date(ts)
          }).zip(values)
       
        val rows = futures.map(pair => Row.fromSeq(Seq(pair._1, pair._2)))
        session.createDataFrame(session.sparkContext.parallelize(rows), schema)
        
      }
      case LongType => {
        
        val recent = times(0).getLong(0)
        /*
         * We shift the most recent past time intervals 
         * into the near future retaining their sequence 
         * in the past
         */    
        val intervals = times.zip(times.tail).map(pair => {
          pair._1.getLong(0) - pair._2.getLong(0)
        }).reverse
        /*
         * Before assigning the recent value to the individual
         * intervals, we have to scan (or sum up) previous values
         */
        val futures = intervals
          .scan(0L) ( _ + _ ).tail
          .map(intval => {      
            val ts = recent + intval
            ts
          }).zip(values)
       
        val rows = futures.map(pair => Row.fromSeq(Seq(pair._1, pair._2)))
        session.createDataFrame(session.sparkContext.parallelize(rows), schema)
        
      }
      case _ => {
        
        val recent = times(0).getTimestamp(0).getTime
        /*
         * We shift the most recent past time intervals 
         * into the near future retaining their sequence 
         * in the past
         */    
        val intervals = times.zip(times.tail).map(pair => {
          pair._1.getTimestamp(0).getTime - pair._2.getTimestamp(0).getTime
        }).reverse
        /*
         * Before assigning the recent value to the individual
         * intervals, we have to scan (or sum up) previous values
         */
        val futures = intervals
          .scan(0L) ( _ + _ ).tail
          .map(intval => {
            val ts = recent + intval
            new java.sql.Timestamp(ts)
          }).zip(values)
       
        val rows = futures.map(pair => Row.fromSeq(Seq(pair._1, pair._2)))
        session.createDataFrame(session.sparkContext.parallelize(rows), schema)
        
      }
    }

  }
  
}

object TSModel extends Load[TSModel] {

}
