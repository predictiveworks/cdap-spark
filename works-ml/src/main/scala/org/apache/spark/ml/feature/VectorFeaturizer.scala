package org.apache.spark.ml.feature
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

import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.util._

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.mutable.WrappedArray

trait VectorFeaturizerParams extends Params {
  
  final val featuresCol = new Param[String](VectorFeaturizerParams.this, "featuresCol",
      "Name of the features field", (value:String) => true)
 
  def setFeaturesCol(value:String): this.type = set(featuresCol, value)
  
  def validateSchema(schema:StructType):Unit = {
    
    val featuresColName = $(featuresCol)  
    
    if (schema.fieldNames.contains(featuresColName) == false)
      throw new IllegalArgumentException(s"Features column $featuresColName does not exist.")
    
  }

}

class VectorFeaturizer(override val uid: String) extends Transformer with VectorFeaturizerParams {
    
  def this() = this(Identifiable.randomUID("vectorFeaturizer"))

  def transform(dataset:Dataset[_]):DataFrame = {
    
    validateSchema(dataset.schema)

    /* Transform features into unique Array[Double] */
    var featureset = dataset.withColumn("_features", columnToArray(dataset, $(featuresCol)))
    
    /* Determine nunber of features */
    val length = featureset.first.getAs[WrappedArray[Double]]("_features").length
    
    (0 until length).foreach(i => {
      featureset = featureset.withColumn(s"f_${i}", col("_features").getItem(i))
    })
    
    featureset.drop("_features")

  }
   
  private def columnToArray(dataset: Dataset[_], colName: String): Column = {
  
    val colDataType = dataset.schema(colName).dataType
    colDataType match {
      case _: VectorUDT =>
        val vectorUDF = udf((v: Vector) => v.toArray)
        vectorUDF(col($(featuresCol)))      
      case fdt: ArrayType =>
        val transferUDF = fdt.elementType match {
          case _: DoubleType => udf((v: Seq[Double]) => v.toArray)
          case _: FloatType => udf((v: Seq[Float]) => {
            val array = Array.ofDim[Double](v.size)
            v.indices.foreach(idx => array(idx) = v(idx).toDouble)
            array
          })
          case _: IntegerType => udf((v: Seq[Int]) => {
            val array = Array.ofDim[Double](v.size)
            v.indices.foreach(idx => array(idx) = v(idx).toDouble)
            array
          })
          case _: LongType => udf((v: Seq[Long]) => {
            val array = Array.ofDim[Double](v.size)
            v.indices.foreach(idx => array(idx) = v(idx).toDouble)
            array
          })
          case other =>
            throw new IllegalArgumentException(s"Array[$other] column cannot be cast to Vector")
        }
        transferUDF(col(colName))
      case other =>
        throw new IllegalArgumentException(s"$other column cannot be cast to Vector")
    }
  }
  
  override def transformSchema(schema:StructType):StructType = {    
    schema    
  }

  override def copy(extra:ParamMap):VectorFeaturizer = defaultCopy(extra)

}