package de.kp.works.dl.transform
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

import org.apache.spark.rdd.RDD

import org.apache.spark.sql._
import org.apache.spark.sql.types._

import com.intel.analytics.bigdl.dataset.Sample
import com.intel.analytics.bigdl.tensor.Tensor
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric.NumericFloat

import com.intel.analytics.bigdl.utils.T

import scala.collection.mutable

object DatasetUtils {
  /*
   * Transform an Apache Spark Dataset into an Analytics Zoo
   * compliant RDD
   */
  def transform(dataset:Dataset[_], features:String, label:String):RDD[Sample[Float]] = {
    /*
     * Restrict the provided dataset to columns `features`
     * and `label
     */
    val source = dataset.select(features, label)
    source.rdd.map(row => {
      
      val schema = row.schema
      /*
       * STEP #1: Transform features into Tensor[Float] and thereby
       * support Array & WrappedArray of Double, Float and Int
       */
      val f = {
        
        val field = schema(features)        

        val idex = schema.fieldIndex(field.name)
        val valu = row.get(idex)
        
        val data = field.dataType match {
          case ArrayType(DoubleType,_) => {
  	          
	          if (valu.isInstanceOf[Array[Double]])
  	              row.getAs[Array[Double]](idex).map(_.toFloat)
  	          
  	          else
                row.getAs[mutable.WrappedArray[Double]](idex).array.map(_.toFloat)
              
          }
		      
  	        case ArrayType(FloatType,_) => {
  	          
	          if (valu.isInstanceOf[Array[Float]])
  	              row.getAs[Array[Float]](idex)
  	          
  	          else
                row.getAs[mutable.WrappedArray[Float]](idex).array
              
  	        }
		      
  	        case ArrayType(IntegerType,_) => {
  	          
	          if (valu.isInstanceOf[Array[Int]])
  	              row.getAs[Array[Int]](idex).map(_.toFloat)
  	          
  	          else
                row.getAs[mutable.WrappedArray[Int]](idex).array.map(_.toFloat)
              
  	        }
  	        
  	        case _ => throw new Exception(s"Data type ${field.dataType.simpleString} is not supported.")
          
        }
        
        val table = T.array(data)
        Tensor[Float](table)
        
      }
      /*
       * STEP #2: Transform label into Tensor[Float] and thereby
       * support Double, Float and Int
       */
      val l = {
        
        val field = schema(label)        

        val idex = schema.fieldIndex(field.name)
        val valu = row.get(idex)
        
        val data = field.dataType match {
          case DoubleType  => row.getAs[Double](idex).toFloat
          case FloatType   => row.getAs[Float](idex)
          case IntegerType => row.getAs[Int](idex).toFloat
  	        
  	        case _ => throw new Exception(s"Data type ${field.dataType.simpleString} is not supported.")
        }
        
        Tensor[Float](T(label))

      }
      /*
       * STEP #3: Finally organize `features`  and `label`
       * as Sample[Float]
       */
      Sample(f, l)

    })
    
  }

}
