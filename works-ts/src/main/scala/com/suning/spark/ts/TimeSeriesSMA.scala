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

import com.suning.spark.transform.Transformer
import com.suning.spark.util.{Identifiable, SaveLoad}

import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
 
class TimeSeriesSMA(override val uid: String, keepOriginal: Boolean,
                    inputCol: String, outputCol: String, timeCol: String, window: Int)
  extends Transformer(uid, keepOriginal) {

  def this(keepOriginal: Boolean, inputCol: String, outputCol: String, timeCol: String,
           window: Int) =
    this(Identifiable.randomUID("TimeSeriesSMA"), keepOriginal, inputCol, outputCol, timeCol,
      window)

  def this(inputCol: String, outputCol: String, timeCol: String, window: Int) =
    this(true, inputCol, outputCol, timeCol, window)

  def this(inputCol: String, timeCol: String, window: Int) =
    this(inputCol, inputCol + "_SMA_" + window, timeCol, window)


  override def transformImpl(df: DataFrame): DataFrame = {
    val n = df.count()
    require(n >= window, s"window can not be larger than number of data instances")

    //     SMA
    val wSpec = Window.orderBy(timeCol).rowsBetween(-window + 1, 0)

    val temp = df.withColumn(outputCol, avg(col(inputCol)).over(wSpec))
    //filter first n( n = offset) rows

    df.sqlContext.createDataFrame(temp.rdd.zipWithIndex().filter(_._2 >= window - 1).map(_._1),
      temp.schema)


    //    WMA to do
    //    EWMA to do
  }

  def forecast(df: DataFrame, numAhead: Int): List[Double] = {

    require(numAhead > 0, s"number of future data instances can not small than 0")

    val SMAdf = transform(df)

    val prefix = "_SMA_"

    var listPrediction = SMAdf.orderBy(desc(timeCol)).select(inputCol + prefix + window)
      .limit(window).collect().map(_.getDouble(0)).toList

    (0 until numAhead).foreach {
      i => {
        listPrediction = listPrediction.slice(0, window).sum / window :: listPrediction
      }
    }

    val prediction = listPrediction.slice(0, numAhead).reverse
    prediction
  }

  override def removeOriginal(df: DataFrame): DataFrame = {
    df.drop(inputCol)
  }

  override def fitImpl(df: DataFrame): this.type = {
    // nothing to do
    this
  }

  override def save(path: String): Unit = {
    TimeSeriesSMA.save(this, path)
  }

  override def saveHDFS(sc: SparkContext, path: String): Unit = {
    TimeSeriesSMA.saveHDFS(sc, this, path)
  }
}

object TimeSeriesSMA extends SaveLoad[TimeSeriesSMA] {

  def apply(uid: String, keepOriginal: Boolean, inputCol: String, outputCol: String,
            timeCol: String, window: Int):
  TimeSeriesSMA = new TimeSeriesSMA(uid, keepOriginal, inputCol, outputCol, timeCol, window)

  def apply(keepOriginal: Boolean, inputCol: String, outputCol: String, timeCol: String,
            window: Int):
  TimeSeriesSMA = new TimeSeriesSMA(keepOriginal, inputCol, outputCol, timeCol, window)

  def apply(inputCol: String, outputCol: String, timeCol: String, window: Int):
  TimeSeriesSMA = new TimeSeriesSMA(inputCol, outputCol, timeCol, window)

  def apply(inputCol: String, timeCol: String, window: Int):
  TimeSeriesSMA = new TimeSeriesSMA(inputCol, timeCol, window)
}
