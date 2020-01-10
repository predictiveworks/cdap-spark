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

import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.util._

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

trait TimeAggregationParams extends Params {
  
  final val timeCol = new Param[String](TimeAggregationParams.this, "timeCol",
      "Name of the timestamp field", (value:String) => true)
   
  final val valueCol = new Param[String](TimeAggregationParams.this, "valueCol",
      "Name of the value field", (value:String) => true)
   
  final val windowDuration = new Param[String](TimeAggregationParams.this, "windowDuration",
      "A string specifying the width of the window, e.g. '10 minutes', '1 second'.", (value:String) => true)

  /**
   * param for aggregation method (supports "avg" (default), "mean", "sum")
   * @group param
   */
  final val aggregationMethod: Param[String] = {

    val allowedParams = ParamValidators.inArray(Array("avg", "mean", "sum"))
    new Param(
      this, "aggregationMethod", "Aggregation method for time series aggregation", allowedParams)
  }
      
  setDefault(aggregationMethod -> "avg", windowDuration -> "10 minutes")
 
  def validateSchema(schema:StructType):Unit = {
    
    /* TIME FIELD */
    
    val timeColName = $(timeCol)  
    
    if (schema.fieldNames.contains(timeColName) == false)
      throw new IllegalArgumentException(s"Time column $timeColName does not exist.")
    
    val timeColType = schema(timeColName).dataType
    if (!(timeColType == DateType || timeColType == LongType || timeColType == TimestampType)) {
      throw new IllegalArgumentException(s"Data type of time column $timeColName must be DateType, LongType or TimestampType.")
    }
    
    /* VALUE FIELD */
    
    val valueColName = $(valueCol)  
    
    if (schema.fieldNames.contains(valueColName) == false)
      throw new IllegalArgumentException(s"Value column $valueColName does not exist.")
    
    val valueColType = schema(valueColName).dataType
    if (valueColType != DoubleType) {
      throw new IllegalArgumentException(s"Data type of value column $valueColName must be a Double.")
    }
    
  }

}

class TimeAggregation(override val uid: String) extends Transformer with TimeAggregationParams {
  
  def this() = this(Identifiable.randomUID("timeAggregation"))

  private val date_to_timestamp = udf {date:java.sql.Date => new java.sql.Timestamp(date.getTime)}

  private val long_to_timestamp = udf {time:Long => new java.sql.Timestamp(time)}

  private val time_to_timestamp = udf {time:java.sql.Timestamp => time}
  
  private val window_to_timestamp = udf {(start:java.sql.Timestamp, end:java.sql.Timestamp) => {

    val center = (start.getTime + end.getTime) / 2
    new java.sql.Timestamp(center)

  }}
  
  def transform(dataset:Dataset[_]):DataFrame = {
    
    validateSchema(dataset.schema)
    /*
     * This transformer operates on a TimestampType column;
     * as a first step, we have to transform the dataset
     */
    val timeset = dataset.schema($(timeCol)).dataType match {

      case DateType => dataset.withColumn("_timestamp", date_to_timestamp(col($(timeCol))))
      case LongType => dataset.withColumn("_timestamp", long_to_timestamp(col($(timeCol))))
      case TimestampType => dataset.withColumn("_timestamp", time_to_timestamp(col($(timeCol))))
      
      case _ => throw new IllegalArgumentException("[TimeAggregation] Unsupported data type detected.")

    }
    /*
     * As a next step sort & aggregate the time series with respect
     * to the internal _timestamp column; the current implementation
     * does not check whether the provided time window duration is
     * valid
     */
    val sorted = dataset.sort(col("_timestamp").asc)
    /*
     * The current implementation supports mean value and sum aggregation
     * of the specified time window
     */
    val method = $(aggregationMethod) match {
      case "avg"  => avg($(valueCol))
      case "mean" => mean($(valueCol))
      case "sum"  => sum($(valueCol))
      case other => throw new IllegalArgumentException(s"[TimeAggregation] Aggregation method '$other' is not supported.")
    }
    
    val aggregated = sorted.groupBy(window(col("_timestamp"), $(windowDuration))).agg(method.as("_value"))
    /*
     * The next step merges the time window columns into an internal
     * _timestamp and _value column
     */
    val merged = aggregated
      .select("window.start", "window.end", "_value")
      .withColumn("_timestamp", window_to_timestamp(col("window.start"), col("window.end")))
      .drop("window.start").drop("window.end")
    /*
     * The timeseries may contain missing values; therefore, this
     * [TimeAggregation] transformer interpolates these missing
     * values from the last non-null value before and the first
     * non-null value after the respective null value 
     */
    merged.withColumnRenamed("_timestamp", $(timeCol)).withColumnRenamed("_value", $(valueCol))

  }
  
  override def transformSchema(schema:StructType):StructType = {    
    schema    
  }

  override def copy(extra:ParamMap):TimeAggregation = defaultCopy(extra)
  
}