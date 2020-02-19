package de.kp.works.core.ml
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
import org.apache.spark.sql.types._
import scala.collection.mutable.WrappedArray
import scala.reflect.api.materializeTypeTag

object MLUtils {
      
  def castToDouble(dataset:Dataset[Row], inputCol:String, castCol:String): Dataset[Row] = {
      dataset.withColumn(castCol, col(inputCol).cast(DoubleType))
  }
  
  def vectorize(dataset:Dataset[Row], featuresCol:String, vectorCol:String): Dataset[Row] = 
    vectorize(dataset, featuresCol, vectorCol, false)
    
  def vectorize(dataset:Dataset[Row], featuresCol:String, vectorCol:String, cast:Boolean = false): Dataset[Row] = {

    /*
     * The dataset contains an Array of Double value (from CDAP structured
     * record) and the classifier, clustering or regression trainer expects
     * a Vector representation
     */
    val vector_udf = udf {features:WrappedArray[Double] => {
      Vectors.dense(features.toArray)
    }}

    if (cast == true) {
      dataset.withColumn(vectorCol, vector_udf(col(featuresCol).cast(ArrayType(DoubleType))))
    
    } else
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
	/*
	 * The type of outputCol is Seq[Vector] where the dimension of the array
	 * equals numHashTables, and the dimensions of the vectors are currently 
	 * set to 1. 
	 * 
	 * In future releases, we will implement AND-amplification so that users 
	 * can specify the dimensions of these vectors.
	 * 
	 * For compliances purposes with CDAP data schemas, we have to resolve
	 * the output format as Array Of Double
	 */
  def flattenMinHash(dataset:Dataset[Row], outputCol:String): Dataset[Row] = {
    
    val flatten_udf = udf {hashes: WrappedArray[Vector] => {
      hashes.toArray.flatMap(vector => vector.toArray)
    }}
    
    dataset.withColumn(outputCol, flatten_udf(col(outputCol)))

  }
  
}

object MLUtilsTest {
  
  def main(args:Array[String]) {
    
    val session = SparkSession.builder
      .appName("MLUtilsTest")
      .master("local")
      .getOrCreate()

    val data = Array(
     Array(1L,2L,3L),
     Array(4L,5L,6L),
     Array(7L,8L,9L)
    )
    
    val df = session.createDataFrame(data.map(Tuple1.apply)).toDF("features")
    val casted = df.withColumn("_casted", col("features").cast(ArrayType(DoubleType)))

    casted.show
  
  }
}