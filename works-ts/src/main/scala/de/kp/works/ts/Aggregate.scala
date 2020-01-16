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
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.catalyst.expressions.TimeWindow

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

trait TimeAggregateParams extends TimeParams {

  final val groupCol = new Param[String](TimeAggregateParams.this, "groupCol",
    "Name of the (optional) group field", (value: String) => true)

  setDefault(groupCol -> null)

  /** @group setParam */
  def setGroupCol(value: String): this.type = set(groupCol, value)

  final val windowDuration = new Param[String](TimeAggregateParams.this, "windowDuration",
    "A string specifying the width of the window, e.g. '10 minutes', '1 second'.", (value: String) => true)

  /** @group setParam */
  def setWindowDuration(value: String): this.type = set(windowDuration, value)
    
  /**
   * param for aggregation method (supports "avg" (default), "count", "mean", "sum")
   * @group param
   */
  final val aggregationMethod: Param[String] = {

    val allowedParams = ParamValidators.inArray(Array("avg", "count", "mean", "sum"))
    new Param(
      this, "aggregationMethod", "Aggregation method for time series aggregation", allowedParams)
  }

  /** @group setParam */
  def setAggregationMethod(value: String): this.type = set(aggregationMethod, value)
 
  setDefault(aggregationMethod -> "avg", windowDuration -> "10 minutes")

}

class Aggregate(override val uid: String) extends Transformer with TimeAggregateParams {

  def this() = this(Identifiable.randomUID("aggregate"))

  private val window_to_timestamp = udf { (start: java.sql.Timestamp, end: java.sql.Timestamp) =>
    {

      val center = (start.getTime + end.getTime) / 2
      new java.sql.Timestamp(center)

    }
  }

  def transform(dataset: Dataset[_]): DataFrame = {

    validateSchema(dataset.schema)
    /*
     * This transformer operates on a TimestampType column;
     * as a first step, we have to transform the dataset
     */
    val timeset = createTimeset(dataset)
    /*
     * As a next step sort & aggregate the time series with respect
     * to the timestamp column
     */
    TSUtils.validateDuration($(windowDuration))
    val sorted = dataset.sort(col($(timeCol)).asc)
    /*
     * The current implementation supports mean value and sum aggregation
     * of the specified time window
     */
    val method = $(aggregationMethod) match {
      case "avg" => avg($(valueCol))
      case "count" => count($(valueCol))
      case "mean" => mean($(valueCol))
      case "sum" => sum($(valueCol))
      case other => throw new IllegalArgumentException(s"[Aggregate] Aggregation method '$other' is not supported.")
    }
    /*
     * Aggregate the time series data by applying a tumbling window of 'windowDuration';
     * note, the resulting dataset contains (optional) 'groupCol', 'timeCol' and 'valueCol'
     */
    val aggregated = if ($(groupCol) == null)
      sorted.groupBy(window(col($(timeCol)), $(windowDuration))).agg(method.as($(valueCol)))

    else
      sorted.groupBy(col($(groupCol)), window(col($(timeCol)), $(windowDuration))).agg(method.as($(valueCol)))

    /*
     * The provided time column is reset to the center of the time window:
     * suppose the initial timestamp specifies 09:00 and the duration is
     * set to 10 minutes, then the window is [09:00, 09:10] and the center
     * is 09:05
     */
    aggregated.withColumn($(timeCol), window_to_timestamp(col("window.start"), col("window.end"))).drop("window")

  }

  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  override def copy(extra: ParamMap): Aggregate = defaultCopy(extra)

}

object AggregateTest {

  private val window_to_timestamp = udf { (start: java.sql.Timestamp, end: java.sql.Timestamp) =>
    {

      println("s:" + start)
      println("e:" + end)
      val center = (start.getTime + end.getTime) / 2
      new java.sql.Timestamp(center)

    }
  }

  def test_func(rows: scala.collection.mutable.WrappedArray[Row]): Array[Row] = {
    rows.map(row => {
      val vs = row.toSeq ++ Seq(1)
      Row.fromSeq(vs)
    }).toArray
  }
  
  def main(args: Array[String]) {

    val schema = StructType(Array(
      StructField("group", StringType, true),
      StructField("ts", StringType, true),
      StructField("value", IntegerType, true),
      StructField("index", StringType, true)))

    val old_schema = StructType(Array(
      StructField("ts", StringType, true),
      StructField("value", IntegerType, true),
      StructField("index", StringType, true)))
    val new_schema = StructType(old_schema.fields ++ Array(StructField("xyz", IntegerType, true)))      
    
    val session = SparkSession.builder
      .appName("AggregateTest")
      .master("local")
      .getOrCreate()

    val data = List(
      Row("A", "01-01-2018 09:00", 1, "a"),
      Row("A", "01-01-2018 09:05", 2, "b"),
      Row("A", "01-01-2018 09:08", 5, "c"),
      Row("A", "01-07-2018 12:00", 10, "d"),
      Row("A", "01-08-2018 13:00", 11, "e"))

    val test_udf = udf(test_func _, DataTypes.createArrayType(new_schema))

    val ds = session.createDataFrame(session.sparkContext.parallelize(data), schema)
    
     val as = ds.groupBy("group").agg(collect_list(struct(Array(col("ts"),col("value"),col("index")): _*)).as("items"))
    
    val rs = as.withColumn("new_items", explode(test_udf(col("items")))).withColumn("ts", col("new_items").getItem("xyz"))
    rs.show
    
  }
}