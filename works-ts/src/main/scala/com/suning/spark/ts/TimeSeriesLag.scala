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
  
class TimeSeriesLag(override val uid: String, keepOriginal: Boolean,
                    inputCol: String, outputCol: String, timeCol: String, offset: Int)
  extends Transformer(uid, keepOriginal) {

  def this(keepOriginal: Boolean, inputCol: String, outputCol: String, timeCol: String,
           offset: Int) =
    this(Identifiable.randomUID("TimeSeriesLag"), keepOriginal, inputCol, outputCol, timeCol,
      offset)

  def this(inputCol: String, outputCol: String, timeCol: String, offset: Int) =
    this(true, inputCol, outputCol, timeCol, offset)

  def this(inputCol: String, timeCol: String, offset: Int) =
    this(inputCol, inputCol + "_lag_" + offset, timeCol, offset)


  override def transformImpl(df: DataFrame): DataFrame = {
    val wSpec = Window.orderBy(timeCol)
    df.withColumn(outputCol, lag(col(inputCol), offset).over(wSpec))
    //.filter( col(outputCol).isNotNull)
  }

  override def removeOriginal(df: DataFrame): DataFrame = {
    df.drop(inputCol)
  }

  override def fitImpl(df: DataFrame): this.type = {
    // nothing to do
    this
  }

  override def save(path: String): Unit = {
    TimeSeriesLag.save(this, path)
  }

  override def saveHDFS(sc: SparkContext, path: String): Unit = {
    TimeSeriesLag.saveHDFS(sc, this, path)
  }
}

object TimeSeriesLag extends SaveLoad[TimeSeriesLag] {
  def apply(uid: String, keepOriginal: Boolean, inputCol: String, outputCol: String,
            timeCol: String, offset: Int):
  TimeSeriesLag = new TimeSeriesLag(uid, keepOriginal, inputCol, outputCol, timeCol, offset)

  def apply(keepOriginal: Boolean, inputCol: String, outputCol: String, timeCol: String,
            offset: Int):
  TimeSeriesLag = new TimeSeriesLag(keepOriginal, inputCol, outputCol, timeCol, offset)

  def apply(inputCol: String, outputCol: String, timeCol: String, offset: Int):
  TimeSeriesLag = new TimeSeriesLag(inputCol, outputCol, timeCol, offset)

  def apply(inputCol: String, timeCol: String, offset: Int):
  TimeSeriesLag = new TimeSeriesLag(inputCol, timeCol, offset)
}
