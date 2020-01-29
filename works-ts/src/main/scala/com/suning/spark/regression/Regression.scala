package com.suning.spark.regression
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

import com.suning.spark.transform.{Feature2Vector, MultiTransformer, String2Index}
import com.suning.spark.util.{Identifiable, Load, Model}

import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DataType, StringType}

abstract class Regression(override val uid: String, featureCols: Array[String], labelCol: String)
  extends Model {
  def this(features: Array[String], label: String) =
    this(Identifiable.randomUID("Regression"), features, label)
  
  override def fit(df: DataFrame): this.type = {
    /*
     * __KUP__ String value support is removed from this class as Suning's original 
     * implementation is limited to Apache Spark's StringIndxer and this is too
     * restrictive here 
     */
    val schema = df.schema
    /*
     * Suning's FeatureVector is backed by Apache Spark's VectorAssembler
     */
    val assembler = Feature2Vector(featureCols, labelCol)
    fitImpl(assembler.transform(df))

  }

  protected def fitImpl(df: DataFrame): this.type

  override def transform(df: DataFrame): DataFrame = {
    /*
     * __KUP__ String value support is removed from this class as Suning's original 
     * implementation is limited to Apache Spark's StringIndxer and this is too
     * restrictive here 
     */
    val schema = df.schema
    /*
     * Suning's FeatureVector is backed by Apache Spark's VectorAssembler
     */
    val assembler = if (!labelCol.isEmpty && schema.fieldNames.contains(labelCol)) {
      Feature2Vector(featureCols, labelCol)
    }
    else {
      Feature2Vector(featureCols)
    }

    val rawDF = assembler.transform(df)
    transformImpl(rawDF)

  }

  protected def transformImpl(df: DataFrame): DataFrame

  override def copy(): Model

  override def save(path: String): Unit

  override def saveHDFS(sc: SparkContext, path: String): Unit
}

object Regression extends Load[Regression]{

}
