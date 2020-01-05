package de.kp.works.ts

/*
 * Copyright (c) 2019 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * This software is the confidential and proprietary information of 
 * Dr. Krusche & Partner PartG ("Confidential Information"). 
 * 
 * You shall not disclose such Confidential Information and shall use 
 * it only in accordance with the terms of the license agreement you 
 * entered into with Dr. Krusche & Partner PartG.
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
    interpolate(merged, $(timeCol), $(valueCol))       

  }
  /*
   * This method supports linear interpolation of missing value
   */
  def interpolate(dataset:DataFrame,timeCol:String, valueCol:String):DataFrame = {

    /* 
     * Create row number over window and a column with 
     * row number only for non missing values 
     */
    val w = Window.partitionBy().orderBy(timeCol)
    /* 
     * Create relative references to the start value 
     * (last value not missing) 
     */
    val wStart = Window.partitionBy().orderBy(timeCol).rowsBetween(Window.unboundedPreceding,-1)
    /* 
     * create relative references to the end value 
     * (first value not missing) 
     */
    val wEnd = Window.partitionBy().orderBy(timeCol).rowsBetween(0, Window.unboundedFollowing)
    /*
     * Define interpolation function
     */
    val interpolation = (col("start_val") + (col("end_val") - col("start_val")) / col("diff_rn") * col("curr_rn"))
    /*
     * Specify drop columns
     */
    val dropColumns = Array("rn", "rn_not_null", "start_val", "end_val", "start_rn", "end_rn", "diff_rn", "curr_rn")
    /*
     * Apply interpolation
     */
    dataset
      .withColumn("rn", row_number().over(w))
      .withColumn("rn_not_null", when(col(valueCol).isNotNull(), col("rn")))
      .withColumn("start_val", last(valueCol,true).over(wStart))
      .withColumn("start_rn", last("rn_not_null",true).over(wStart))
      .withColumn("end_val", first(valueCol, true).over(wEnd))
      .withColumn("end_rn", first("rn_not_null", true).over(wEnd))
      /* 
       * Create references to gap length and current gap position
       */
      .withColumn("diff_rn", col("end_rn") - col("start_rn"))
      .withColumn("curr_rn", col("diff_rn") - (col("end_rn") - col("rn")))
      /*
       * Calculate interpolation value
       */
      .withColumn(valueCol, when(col(valueCol).isNull(), interpolation).otherwise(col(valueCol)))
      .drop(dropColumns: _*)

  }
  
  override def transformSchema(schema:StructType):StructType = {    
    schema    
  }

  override def copy(extra:ParamMap):TimeAggregation = defaultCopy(extra)
  
}