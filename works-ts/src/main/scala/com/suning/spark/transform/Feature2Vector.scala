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
import org.apache.spark.ml.feature.{VectorAssembler => SparkVA}
import org.apache.spark.sql.DataFrame

class Feature2Vector(override val uid: String, keepOriginal: Boolean,
                     features: Array[String], label: String)
  extends Transformer(uid, keepOriginal) {

  def this(keepOriginal: Boolean, features: Array[String], label: String) =
    this(Identifiable.randomUID("Feature2Vector"), keepOriginal, features, label)

  def this(features: Array[String], label: String) =
    this(Identifiable.randomUID("Feature2Vector"), true, features, label)

  protected val va = new SparkVA().setInputCols(features).setOutputCol("features")

  override def transformImpl(df: DataFrame): DataFrame = {
    if( label == "") {
      va.transform(df)
    } else {
      va.transform(df).withColumn("label", df(label))
    }
  }

  override def removeOriginal(df: DataFrame): DataFrame = {
    val cols = df.schema.fieldNames.filter(!features.contains(_))
    df.select(cols.head, cols.tail: _*)
  }

  override def fitImpl(df: DataFrame): this.type = {
    this
  }

  override def save(path: String): Unit ={
    Feature2Vector.save(this, path)
  }

  override def saveHDFS(sc: SparkContext, path: String): Unit = {
    Feature2Vector.saveHDFS(sc, this, path)
  }
}

object Feature2Vector extends SaveLoad[Feature2Vector]{
  def apply(uid: String, keepOriginal: Boolean, features: Array[String], label: String):
  Feature2Vector = new Feature2Vector(uid, keepOriginal, features, label)

  def apply(keepOriginal: Boolean, features: Array[String], label: String):
  Feature2Vector = new Feature2Vector(keepOriginal, features, label)

  def apply(features: Array[String], label: String):
  Feature2Vector = new Feature2Vector(features, label)

  def apply(features: Array[String]):
  Feature2Vector = new Feature2Vector(features, "")
}
