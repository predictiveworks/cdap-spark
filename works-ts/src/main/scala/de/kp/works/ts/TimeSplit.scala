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
import org.apache.spark.sql.functions._

import org.apache.spark.sql.types._

import scala.collection.mutable.HashMap

class TimeSplit {  
    /*
     * This data structure holds the parameters
     */
    private val params = HashMap.empty[String, Any]
    
    private val date2Timestamp = udf {date:java.sql.Date => new java.sql.Timestamp(date.getTime)}

    private val long2Timestamp = udf {time:Long => new java.sql.Timestamp(time)}

    private val time2Timestamp = udf {time:java.sql.Timestamp => time}
    
    /*
     * A helper method to set the time column of the dataset
     */
    def setTimeCol(timeCol:String): TimeSplit = {
      
      params += "timeCol" -> timeCol
      this
      
    }    
    /*
     * A helper method to set the time split of the dataset
     */
    def setTimeSplit(timeSplit:String): TimeSplit = {
      
      params += "timeSplit" -> timeSplit
      this
      
    }
    
    def timeSplit(dataset:Dataset[_]): Array[Dataset[Row]] = {
      
      validate()

      /* Check data time of time col and transform into Long */
      
      /* Compute splits */
      val timeSplit = params("timeSplit").asInstanceOf[String]        
      
      val fractions = timeSplit.split(":").map(token => {
          token.trim.toDouble / 100
      })
      
      if (fractions.size != 2 || fractions.sum != 1D)
        throw new IllegalArgumentException("[TimeSplit] The time split expects two integer numbers of sum 100.")
      
      /* Append extra collumn '_time' */ 
      val timeCol = params("timeCol").asInstanceOf[String]
      
      val timecol = col(timeCol)
      val timeset = dataset.schema(timeCol).dataType match {

        case DateType      => dataset.withColumn("_time", date2Timestamp(timecol)).sort(col("_time"))
        case LongType      => dataset.withColumn("_time", long2Timestamp(timecol)).sort(col("_time"))
        case TimestampType => dataset.withColumn("_time", time2Timestamp(timecol)).sort(col("_time"))
      
        case _ => throw new IllegalArgumentException("[TimeSplit] Unsupported time data type detected.")

      }
      
      val maxTime = timeset.agg({"_time" -> "max"}).collect.head(0).asInstanceOf[Long] 
      val minTime = timeset.agg({"_time" -> "min"}).collect.head(0).asInstanceOf[Long] 
      
      /* Compute threshold */
      val threshold = minTime + Math.ceil(fractions(0) * (maxTime - minTime)).toLong 
      
      /* Split timeset */
      val lowerSplit = timeset.filter(col("_time") <= threshold).drop("_time")
      val upperSplit = timeset.filter(col("_time") > threshold).drop("_time")
      
      Array(lowerSplit, upperSplit)

    }

    private def validate():Unit = {
        
        val timeCol = params.getOrElse("timeCol", null)        

        if (timeCol == null || timeCol.asInstanceOf[String].isEmpty)
          throw new IllegalArgumentException("[TimeSplit] The name of time column must be provided.")
                
        val timeSplit = params.getOrElse("timeSplit", null)        

        if (timeSplit == null || timeSplit.asInstanceOf[String].isEmpty)
          throw new IllegalArgumentException("[TimeSplit] The time split must be provided.")
 
    }
}