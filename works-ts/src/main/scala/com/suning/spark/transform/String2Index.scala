package com.suning.spark.transform
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

import com.suning.spark.util.{Identifiable, SaveLoad}

import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.{StringIndexer => SparkStringIndexer}
import org.apache.spark.ml.feature.{StringIndexerModel => SparkStringIndexerModel}
import org.apache.spark.sql.DataFrame

class String2Index(override val uid: String, keepOriginal: Boolean,
                   inputCol: String, outputCol: String)
  extends Transformer(uid, keepOriginal) {

  def this(keepOriginal: Boolean, inputCol: String, outpuCol: String) =
    this(Identifiable.randomUID("String2Index"), keepOriginal, inputCol, outpuCol)

  def this(inputCol: String, outputCol: String) =
    this(true, inputCol, outputCol)

  private val si = new SparkStringIndexer()
    .setInputCol(inputCol).setOutputCol(outputCol)
  private var siModel: SparkStringIndexerModel = _

  var labels: Array[String] = _

  override def transformImpl(df: DataFrame): DataFrame = {
    if( siModel == null){
      fit(df)
    }
    siModel.transform(df)
  }

  override def removeOriginal(df: DataFrame): DataFrame = {
    df.drop(inputCol)
  }

  override def fitImpl(df: DataFrame): this.type = {
    siModel = si.fit(df)
    labels = siModel.labels
    this
  }

  override def save(path: String): Unit ={
    String2Index.save(this, path)
  }

  override def saveHDFS(sc: SparkContext, path: String): Unit = {
    String2Index.saveHDFS(sc, this, path)
  }

}

object String2Index extends SaveLoad[String2Index] {
  def apply(uid: String, keepOriginal: Boolean, inputCol: String, outputCol: String):
  String2Index = new String2Index(uid, keepOriginal, inputCol, outputCol)

  def apply(keepOriginal: Boolean, inputCol: String, outputCol: String):
  String2Index = new String2Index(keepOriginal, inputCol, outputCol)

  def apply(inputCol: String, outputCol: String):
  String2Index = new String2Index(inputCol, outputCol)
}
