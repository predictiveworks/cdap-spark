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

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.util._

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.{Window, WindowSpec}

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.JavaConversions._

trait TimeInterpolateParams extends TimeParams {
  
  final val groupCol = new Param[String](TimeInterpolateParams.this, "groupCol",
      "Name of the (optional) group field", (value:String) => true)
 
  setDefault(groupCol -> null)
 
  /** @group setParam */
  def setGroupCol(value:String): this.type = set(groupCol, value)
  
  override def validateSchema(schema:StructType):Unit = {
    super.validateSchema(schema)
    
  }
  
}
/**
 * This transformer operates on a timeseries and interpolates
 * missing values from the last non-null value before and the 
 * first on-null value after the respective null value.
 */
class Interpolate(override val uid: String) extends Transformer with TimeInterpolateParams {
  
  def this() = this(Identifiable.randomUID("interpolate"))

  private def rowNumberSpec(): WindowSpec = {
    
    if ($(groupCol) == null)
      Window.partitionBy().orderBy($(timeCol))
      
    else 
      Window.partitionBy($(groupCol)).orderBy($(timeCol))
    
  }
  
  private def fillForwardSpec(): WindowSpec = {
    
    if ($(groupCol) == null)
      Window.partitionBy().orderBy($(timeCol)).rowsBetween(Window.unboundedPreceding, -1)
      
    else 
      Window.partitionBy($(groupCol)).orderBy($(timeCol)).rowsBetween(Window.unboundedPreceding, -1)
    
  }
  
  private def fillBackwardSpec(): WindowSpec = {
    
    if ($(groupCol) == null)
      Window.partitionBy().orderBy($(timeCol)).rowsBetween(0, Window.unboundedFollowing)
      
    else 
      Window.partitionBy($(groupCol)).orderBy($(timeCol)).rowsBetween(0, Window.unboundedFollowing)
    
  }

  def transform(dataset:Dataset[_]):DataFrame = {
    
    validateSchema(dataset.schema)
    /*
     * This transformer operates on a TimestampType column;
     * as a first step, we have to transform the dataset
     */
    val timeset = createTimeset(dataset)
    /*
  	   * Define interpolation function
     */
    val interpolation = (col("start_val") + (col("end_val") - col("start_val")) / col("diff_rn") * col("curr_rn"))
    /*
     * Specify drop columns
     */
    val dropColumns = Array("rn", "rn_not_null", "start_val", "end_val", "start_rn", "end_rn", "diff_rn", "curr_rn")
    
    val valuecol = col($(valueCol))
    timeset
      /*
       * Make sure that the value is a double value, whatever
       * numeric type is used
       */
      .withColumn($(valueCol), valuecol.cast(DoubleType))
      /*
       * Add a row number to each record and indicate with another
       * column those that contain non-null values
       */
      .withColumn("rn", row_number().over(rowNumberSpec))
      .withColumn("rn_not_null", when(valuecol.isNotNull, col("rn")))
      /* fill forward with ignore nulls: true */
      .withColumn("start_val", last($(valueCol),true).over(fillForwardSpec))
      .withColumn("start_rn", last("rn_not_null",true).over(fillForwardSpec))
      /* fill backward with ignore nulls: true */
      .withColumn("end_val", first($(valueCol), true).over(fillBackwardSpec))
      .withColumn("end_rn", first("rn_not_null", true).over(fillBackwardSpec))
      /* 
       * Create references to gap length and current gap position
       */
      .withColumn("diff_rn", col("end_rn") - col("start_rn"))
      .withColumn("curr_rn", col("diff_rn") - (col("end_rn") - col("rn")))
      /*
       * Calculate interpolation value
       */
      .withColumn($(valueCol), when(valuecol.isNull, interpolation).otherwise(valuecol))
      .drop(dropColumns: _*)
    
  }
  
  override def transformSchema(schema:StructType):StructType = {    
    schema    
  }

  override def copy(extra:ParamMap):Interpolate = defaultCopy(extra)
  
}

object InterpolateTest {
  
  def main(args: Array[String]) {

    val session = SparkSession.builder
      .appName("InterpolateTest")
      .master("local")
      .getOrCreate()
    
    val data = List(
      Row("A","01-01-2018",1),
      Row("A","01-02-2018",2),
      Row("A","01-03-2018",null),
      Row("A","01-04-2018",null),
      Row("A","01-05-2018",5),
      Row("A","01-06-2018",null),
      Row("A","01-07-2018",10),
      Row("A","01-08-2018",11)
     )
     
     val schema = StructType(Array(
         StructField("group", StringType, true),
         StructField("ts", StringType, true),
         StructField("value", IntegerType, true)
         ))
     
     val ds = session.createDataFrame(session.sparkContext.parallelize(data), schema)
     val df = ds.withColumn("ts", unix_timestamp(col("ts"),"MM-dd-yyyy").cast("timestamp"))
/*
+-----+--------------------+-----+
|group|                  ts|value|
+-----+--------------------+-----+
|    A|2018-01-01 00:00:...|    1|
|    A|2018-01-02 00:00:...|    2|
|    A|2018-01-03 00:00:...| null|
|    A|2018-01-04 00:00:...| null|
|    A|2018-01-05 00:00:...|    5|
|    A|2018-01-06 00:00:...| null|
|    A|2018-01-07 00:00:...|   10|
|    A|2018-01-08 00:00:...|   11|
+-----+--------------------+-----+
 *     
 */
     df.show

     val interpolator = new Interpolate().setGroupCol("group").setTimeCol("ts").setValueCol("value")
     val rs = interpolator.transform(df)
/*
+-----+--------------------+-----+
|group|                  ts|value|
+-----+--------------------+-----+
|    A|2018-01-01 00:00:...|  1.0|
|    A|2018-01-02 00:00:...|  2.0|
|    A|2018-01-03 00:00:...|  3.0|
|    A|2018-01-04 00:00:...|  4.0|
|    A|2018-01-05 00:00:...|  5.0|
|    A|2018-01-06 00:00:...|  7.5|
|    A|2018-01-07 00:00:...| 10.0|
|    A|2018-01-08 00:00:...| 11.0|
+-----+--------------------+-----+
 *      
 */
     rs.show
     
  }
}