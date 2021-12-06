package de.kp.works.ts
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

import org.apache.spark.ml.param._

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.util._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

trait TimeResampleParams extends TimeParams {
  /*
   * Suppose a time series contains values that belong to different categories or groups,
   * say sensor devices, then resampling is performed for each group
   */
  final val groupCol = new Param[String](TimeResampleParams.this, "groupCol",
    "Name of the (optional) group field", (_:String) => true)

  final val stepSize = new Param[Int](TimeResampleParams.this, "stepSize",
    "Distance between subsequent points of the time series after resampling in seconds.", (value:Int) => true)

  setDefault(groupCol -> null)

  /** @group setParam */
  def setGroupCol(value:String): this.type = set(groupCol, value)

  /** @group setParam */
  def setStepSize(value:Int): this.type = set(stepSize, value)

  override def validateSchema(schema:StructType):Unit = {
    super.validateSchema(schema)

  }

}

class Resample(override val uid: String) extends Transformer with TimeResampleParams {

  def this() = this(Identifiable.randomUID("resample"))

  /*
   * __KUP__
   *
   * Time interval is set to [Long]
   *
   * A helper method to return a list of equally spaced
   * points between ts1 and ts2 with step size 'step'.
   */
  private def timegrid_udf(interval:Long) = udf {(ts1:Long, ts2:Long) => {

    val steps = ((ts2 -ts1) / interval).toInt + 1
    (0 until steps).map(x => ts1 + (x * interval)).toArray

  }
  }

  def transform(dataset:Dataset[_]):DataFrame = {

    validateSchema(dataset.schema)
    /*
     * This transformer operates on a TimestampType column;
     * we ensure that the time column is formatted as a time
     * stamp.
     *
     * For further processing, we additionally have to turn
     * the time column into a (reversible) LongType
     */
    val timeset = createTimeset(dataset)
      .withColumn($(timeCol), col($(timeCol)).cast(LongType))

    /*
     * The stepSize (or time interval) is provided in seconds.
     * It is important to transform into [Long]
     */
    val interval = ($(stepSize) * 1000).toLong
    val grid = timegrid_udf(interval)

    val timegrid = if ($(groupCol) == null) {
      /*
       * The time series does not contain different categories
       * or groups of data points
       */
      val baseset = timeset
        /*
         * Determine the minimum & maximum value of the time series
         */
        .agg(min($(timeCol)).cast(LongType).as("mints"), max($(timeCol)).cast(LongType).as("maxts"))
        /*
         * Compute timegrid and explode values
         */
        .withColumn($(timeCol), explode(grid(col("mints"), col("maxts")))).drop("mints").drop("maxts")

      baseset.join(timeset, Seq($(timeCol)), "left_outer").sort(col($(timeCol)).asc)

    } else {

      val baseset = timeset
        /*
         * Determine the minimum & maximum value of the time series
         */
        .groupBy($(groupCol)).agg(min($(timeCol)).cast(LongType).as("mints"), max($(timeCol)).cast(LongType).as("maxts"))
        /*
         * Compute timegrid and explode values
         */
        .withColumn($(timeCol), explode(grid(col("mints"), col("maxts")))).drop("mints").drop("maxts")

      baseset.join(timeset, Seq($(groupCol), $(timeCol)), "left_outer").sort(col($(timeCol)).asc)

    }

    timegrid.withColumn($(timeCol), long_to_timestamp(col($(timeCol))))

  }

  override def transformSchema(schema:StructType):StructType = {
    schema
  }

  override def copy(extra:ParamMap):Interpolate = defaultCopy(extra)

}

object ResampleTest {

  def main(args: Array[String]) {

    val session = SparkSession.builder
      .appName("ResampleTest")
      .master("local")
      .getOrCreate()

    val data = List(
      Row("A","01-15-2018",1),
      Row("A","01-16-2018",2),
      Row("A","01-17-2018",null),
      Row("A","01-20-2018",null),
      Row("A","01-23-2018",5),
      Row("A","01-24-2018",null),
      Row("A","01-25-2018",10),
      Row("A","01-29-2018",11)
    )

    val schema = StructType(Array(
      StructField("group", StringType, nullable = true),
      StructField("ts", StringType, nullable = true),
      StructField("value", IntegerType, nullable = true)
    ))

    val ds = session.createDataFrame(session.sparkContext.parallelize(data), schema)
    val df = ds.withColumn("ts", unix_timestamp(col("ts"),"MM-dd-yyyy").cast("timestamp"))

    val resampler = new Resample().setGroupCol("group").setTimeCol("ts").setValueCol("value")
    val rs = resampler.transform(df)

    rs.show

  }
}
