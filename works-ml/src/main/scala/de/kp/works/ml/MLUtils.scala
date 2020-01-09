package de.kp.works.ml
/*
 * Copyright (c) 2019 Dr. Krusche & Partner PartG. All rights reserved.
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

import org.apache.spark.ml.linalg.{Vector, Vectors}

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.collection.mutable.WrappedArray

object MLUtils {
      
  def vectorize(dataset:Dataset[Row], featuresCol:String, vectorCol:String): Dataset[Row] = {
    /*
     * The dataset contains an Array of Double value (from CDAP structured
     * record) and the classifier, clustering or regression trainer expects
     * a Vector representation
     */
    val vector_udf = udf {features:WrappedArray[Double] => {
      Vectors.dense(features.toArray)
    }}

    dataset.withColumn(vectorCol, vector_udf(col(featuresCol)))
    
  }
       
  def devectorize(dataset:Dataset[Row], vectorCol:String, featureCol:String): Dataset[Row] = {
    /*
     * The dataset contains an Apache Spark Vector and must be transformed
     * into an Array of Double value (to CDAP Structured Record)
     */
    val devector_udf = udf {vector:Vector => {
      vector.toArray
    }}

    dataset.withColumn(featureCol, devector_udf(col(vectorCol)))
    
  }
 
}