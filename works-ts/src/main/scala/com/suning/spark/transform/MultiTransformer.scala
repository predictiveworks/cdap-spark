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
import org.apache.spark.sql.DataFrame

class MultiTransformer(override val uid: String, transformers: Array[Transformer] )
  extends Transformer (uid) {
  def this(transformers: Array[Transformer]) =
    this(Identifiable.randomUID("MultiTransformer"), transformers)

  override def transformImpl(df: DataFrame): DataFrame = {
    transformers.foldLeft(df)((d, t) => t.transform(d))
  }

  override def fitImpl(df: DataFrame): this.type = {
    transformers.foreach(_.fit(df))
    this
  }

  override def removeOriginal(df: DataFrame): DataFrame = {
    df
  }

  override def save(path: String): Unit ={
    MultiTransformer.save(this, path)
  }

  override def saveHDFS(sc: SparkContext, path: String): Unit = {
    MultiTransformer.saveHDFS(sc, this, path)
  }
}

object MultiTransformer extends SaveLoad[MultiTransformer] {
  def apply(transformers: Array[Transformer]): MultiTransformer =
    new MultiTransformer(transformers)
}
