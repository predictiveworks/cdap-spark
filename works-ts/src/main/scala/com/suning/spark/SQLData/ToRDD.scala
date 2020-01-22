package com.suning.spark.SQLData
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

import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame,Row}

object ToRDD {

  def toRDDVector(df:DataFrame):RDD[Vector] = {
    val dataResult = df.rdd.map(row => {
      Vectors.dense(row.toSeq.toArray.map(
        {
          case s: String => s.toDouble
          case l: Long => l.toDouble
          case d: Double => d.toDouble
          case i: Int => i.toDouble
          case f: Float => f.toDouble
          case b:Byte => b.toDouble
          case _ => 0.0
        }
      ))
    })
    dataResult
  }

  def toRddRow(df:DataFrame):RDD[Row] = {

    val dataResult = df.rdd.map(row => {
      Row.fromSeq(row.toSeq.toArray.map(
        {
          case s: String => s.toDouble
          case l: Long => l.toDouble
          case d: Double => d.toDouble
          case i: Int => i.toDouble
          case f: Float => f.toDouble
          case b: Byte => b.toDouble
          case _ => 0.0
        }
      ))
    })
    dataResult

  }

}
