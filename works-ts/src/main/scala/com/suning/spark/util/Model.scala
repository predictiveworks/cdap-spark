package com.suning.spark.util
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

import com.suning.spark.Logging

import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
  
abstract class Model(override val uid: String)
  extends Logging with Identifiable with Serializable {
  def this() =
    this(Identifiable.randomUID("Model"))


  def fit(df: DataFrame): this.type = {
    fitImpl(df)
  }

  protected def fitImpl(df: DataFrame): this.type

  def transform(df: DataFrame): DataFrame = {
    transformImpl(df)
  }

  protected def transformImpl(df: DataFrame): DataFrame

  def save(path: String): Unit

  def saveHDFS(sc: SparkContext, path: String): Unit

  def copy(): Model
}

object Model extends Load[Model] {

}
