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

import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.mllib.linalg.{Vector, VectorUDT, Vectors}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object VectorUtil {

  val s2v = udf((string: String) => {
    Vectors.parse(string)
  })

  val v2s = udf((features: Vector) => {
    features.toString
  })

  def string2Vector(df: DataFrame): DataFrame = {
    val schema = df.schema
    var newDF = df
    schema.foreach( field => {
      field.dataType match {
        case _: StringType => {
          val colName = field.name
          if (colName.startsWith("v2s_")) {
            val newColName = colName.replace("v2s_", "")
            newDF = newDF.withColumn(newColName, s2v(newDF(colName))).drop(newDF(colName))
          }
        }
        case _: DataType => {
        }
      }
    })
    newDF
  }

  def vector2String(df: DataFrame): DataFrame = {
    val schema = df.schema
    var newDF = df
    schema.foreach( field => {
      field.dataType match {
        case _: VectorUDT => {
          val colName = field.name
          val newColName = "v2s_" + field.name
          newDF = newDF.withColumn(newColName, v2s(newDF(colName))).drop(newDF(colName))
        }
        case _: DataType => {
        }
      }
    })
    newDF
  }
}
