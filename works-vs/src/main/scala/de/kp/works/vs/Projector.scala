package de.kp.works.vs

/*
 * Copyright (c) 2019 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
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
 *
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 *
 */

import de.kp.works.core.recording.MLUtils
import java.util.{List => JList}

import org.apache.spark.ml.feature.PCA
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.JavaConversions._

object Projector {

  def execute(source:Dataset[Row], featuresCol:String, otherCols:JList[String]):Dataset[Row] = {

    val vectorCol = "_vector"
    /*
     * Prepare provided dataset by vectorizing the features column
     * which is specified as Array[Numeric]
     */
    val vectorset = MLUtils.vectorize(source, featuresCol, vectorCol, cast = true)
    /*
     * Restrict to columns that are required for visualization
     */
    val selectCols = (Seq(featuresCol) ++ otherCols).map(col)
    val prepareset = vectorset.select(selectCols: _*)
    /*
     * Train PCA model and apply projection to prepared dataset
     */
    val projectedCol = "_projected"

    val model = new PCA()
      .setInputCol(featuresCol)
      .setOutputCol(projectedCol)
      .setK(2)
      .fit(prepareset)

    val projected = model
      .transform(prepareset)
      .select(selectCols: _*)

    val decompose_udf = udf((vector:DenseVector) => {
        val point = vector.toArray
        Seq(point(0), point(1))
    })
    /*
     * The result is a 3-column dataset, x, y, prediction
     */
    projected
      .withColumn("_point", decompose_udf(col(projectedCol)))
      .withColumn("x", col("_point").getItem(0))
      .withColumn("y", col("_point").getItem(1))
      .drop("_point", projectedCol)

  }
}
