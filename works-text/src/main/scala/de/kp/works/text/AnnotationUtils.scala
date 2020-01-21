package de.kp.works.text
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
import java.util.{List => JList, Map => JMap}
import java.lang.reflect.Type

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.mutable.WrappedArray

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object AnnotationUtils extends Serializable {

  private val gson = new Gson()
  private	val jsonType = new TypeToken[JMap[String, Object]](){}.getType()
  
  def serializeAnnotations(dataset:Dataset[Row], annotationCol:String):Dataset[Row] = {
     dataset.withColumn(annotationCol, toGson(col(annotationCol)))
  }
  
  def deserializeAnnotations(dataset:Dataset[Row], annotationCol:String, tempCol:String, annotatorType:String="document"):Dataset[Row] = {
    
    val metadataBuilder = new MetadataBuilder()
    metadataBuilder.putString("annotatorType", annotatorType)
    
    val metadata = metadataBuilder.build()    
    dataset.withColumn(tempCol, fromGson(col(annotationCol)).as("_", metadata))
  
  }
  
  /*
   * A helper method to transform the result of an annotation stage
   * into a serialized Gson format for further processing
   */
  def toGson = udf { annotations: WrappedArray[Row] => {
    
    annotations.map(c => {
      
      val schema = c.schema
      val data = new java.util.HashMap[String,Object]()
      
      data.put("annotatorType", c.getString(schema.fieldIndex("annotatorType")))
      data.put("begin"        , c.getInt(schema.fieldIndex("begin")).asInstanceOf[Object])     
      data.put("end"          , c.getInt(schema.fieldIndex("end")).asInstanceOf[Object])
      data.put("result"       , c.getString(schema.fieldIndex("result")))
      data.put("metadata"     , c.getJavaMap[String,String](schema.fieldIndex("metadata")))
      data.put("embeddings"   , c.getList[Float](schema.fieldIndex("embeddings")))

      gson.toJson(data)

    }).toArray
    
  }}
  
  private def gson2Row(json: WrappedArray[String]): Array[Row] = {

    json.map(c => {
      
      val data = gson.fromJson(c, jsonType).asInstanceOf[JMap[String, Object]]      
      val annotatorType = data.get("annotatorType").asInstanceOf[String]
      /*
       * Gson transforms [Int] into [Double]
       */
      val begin = data.get("begin").asInstanceOf[Double].toInt
      val end = data.get("end").asInstanceOf[Double].toInt

      val result = data.get("result").asInstanceOf[String]
      val metadata = data.get("metadata").asInstanceOf[JMap[String,String]].toMap
      
      val embeddings = data.get("embeddings").asInstanceOf[JList[Float]].toArray
      Row.fromSeq(Seq(annotatorType, begin, end, result, metadata, embeddings))

    }).toArray
    
  }
/*
 *   /** requirement for pipeline transformation validation. It is called on fit() */
  override final def transformSchema(schema: StructType): StructType = {
    val metadataBuilder: MetadataBuilder = new MetadataBuilder()
    metadataBuilder.putString("annotatorType", outputAnnotatorType)
    val outputFields = schema.fields :+
      StructField(getOutputCol, ArrayType(Annotation.dataType), nullable = false, metadataBuilder.build)
    StructType(outputFields)
  }
 * 
 */
  def fromGson = udf(gson2Row _, DataTypes.createArrayType(com.johnsnowlabs.nlp.Annotation.dataType))
   
}