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

import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}

import com.suning.spark.Logging
import com.suning.spark.util.{Identifiable, Load, SaveLoad}

import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkContext

abstract class Transformer(override val uid: String, keepOriginal: Boolean = true)
  extends Logging with Identifiable with Serializable {

  def this(keepOriginal: Boolean) =
    this(Identifiable.randomUID("Transformer"), keepOriginal)

  def this() =
    this(Identifiable.randomUID("Transformer"), true)

  def transform(df: DataFrame): DataFrame = {
    if (keepOriginal) transformImpl(df) else removeOriginal(transformImpl(df))
  }

  def transformImpl(df: DataFrame): DataFrame

  def fit(df: DataFrame): this.type = {
    fitImpl(df)
  }

  protected def fitImpl(df: DataFrame): this.type

  def removeOriginal(df: DataFrame): DataFrame

  def save(path: String): Unit

  def saveHDFS(sc: SparkContext, path: String): Unit
}


object Transformer extends Load[Transformer]{

}
