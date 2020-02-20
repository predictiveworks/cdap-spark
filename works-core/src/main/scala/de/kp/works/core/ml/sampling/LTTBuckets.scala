package de.kp.works.core.ml.sampling
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

import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.util._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

/**
 * This implementation of the Largest Triangle Three Buckets (LTTB)
 * algorithm is NOT applicable to distributed large scale datasets.
 * 
 * It exclusively processes data on the master node. However, it is
 * implemented as an Apache Spark ML transformer to be easily combined
 * with other computation stages
 */

trait LTTBucketsParams extends Params {
 
  final val xCol = new Param[String](this, "xCol",
      "Name of the independent value field", (value:String) => true)
   
  final val yCol = new Param[String](this, "yCol",
      "Name of the dependent value field", (value:String) => true)
 
  def setxCol(value:String): this.type = set(xCol, value)
 
  def setyCol(value:String): this.type = set(yCol, value)
    
  final val sampleSize: Param[Int] = new Param[Int](this, "sampleSize",
      "The size of the downsampled dataset.", (v: Int) => ParamValidators.gt(2)(v))
       
  def setSampleSize(value:Int): this.type = set(sampleSize, value)
   
  def validateSchema(schema:StructType):Unit = {
    
    /* X FIELD */
    
    val xColName = $(xCol)  
    
    if (schema.fieldNames.contains(xColName) == false)
      throw new IllegalArgumentException(s"'x' column $xColName does not exist.")
    
    val xColType = schema(xColName).dataType
    xColType match {
      case DoubleType | FloatType | IntegerType | LongType | ShortType =>
      case _ => throw new IllegalArgumentException(s"Data type of value column $xColName must be a numeric type.")
    }
    
    /* VALUE FIELD */
    
    val yColName = $(yCol)  
    
    if (schema.fieldNames.contains(yColName) == false)
      throw new IllegalArgumentException(s"'y' column $yColName does not exist.")
    
    val yColType = schema(yColName).dataType
    yColType match {
      case DoubleType | FloatType | IntegerType | LongType | ShortType =>
      case _ => throw new IllegalArgumentException(s"Data type of value column $yColName must be a numeric type.")
    }
    
  }
  
  protected def createSampleset(dataset:Dataset[_]):Dataset[Row] = {

    dataset
      .withColumn($(xCol), col($(xCol)).cast(DoubleType))
      .withColumn($(yCol), col($(yCol)).cast(DoubleType))
  
  } 

}

class LTTBuckets(override val uid: String) extends Transformer with LTTBucketsParams {
    
  def this() = this(Identifiable.randomUID("LTTBuckets"))
  
  private case class Point(x:Double, y:Double)
  
  def transform(dataset:Dataset[_]):DataFrame = {
    
    val schema = dataset.schema
    validateSchema(dataset.schema)
    
    val sampleset = createSampleset(dataset)
    
    /*
     * Transfer dataset to the master node and apply the 
     * Largest Triangle Three Buckets (LTTB)
     */
    val xIdx = schema.fieldIndex($(xCol))
    val yIdx = schema.fieldIndex($(yCol))
    
    val data = sampleset.select($(xCol),$(yCol)).collect.map(row => {

        val x = row.getDouble(xIdx)
        val y = row.getDouble(yIdx)
        
        Point(x,y)
        
    })
    /*
     * Apply LTTBuckets algorithm and retransform
     * samples into dataframe
     */
    val sampled = doLTTBuckets(data, $(sampleSize)).map(p => (p.x, p.y))
    val output = dataset.sparkSession.createDataFrame(sampled).toDF($(xCol), $(yCol)) 
    
    val xType = schema($(xCol)).dataType
    val yType = schema($(yCol)).dataType
    
    output
      .withColumn($(xCol), col($(xCol)).cast(xType))
      .withColumn($(yCol), col($(yCol)).cast(yType))
    
  }
  
  private def doLTTBuckets(data:Array[Point], sampleSize:Int): Array[Point] = {

    /*
     * First, check whether the size of the data is below 
     * the provided threshold; in this case, do nothing
     */
    val dataSize = data.length;
    if (sampleSize >= dataSize || sampleSize == 0) {
        return data
    }
    
    val sampled = ArrayBuffer.empty[Point]
    val bucketSize = (dataSize - 2).toDouble / (sampleSize - 2)
  
    /*
     * A step by step overview of the LTTB algorithm:
     * 
     * The data set is divided into buckets. The number of buckets will 
     * determine the size of the data set after sampling.
     * 
     * (1) The first bucket contains only one data point, namely the first.
     * 
     * (2) The last bucket contains only one data point, namely the last.
     * 
     * (3) All other data points are divided over the other buckets.
     * 
     * The algorithm will select for each of the buckets one data point such 
     * that the surface of the triangle formed by the data point selected from 
     * the 'previous’ bucket, the data point selected from the 'current’ bucket 
     * and the data point selected from the 'following’ bucket is maximized.
     * 
     * Because the first bucket contains only one data point and the algorithm 
     * moves from 'left to right', determining the selected data point of the 
     * 'previous’ bucket is easy. 
     * 
     * When working on the second bucket, it’s the only data point of the first 
     * bucket. For the nth bucket, it’s the data point selected in the previous 
     * step of the algorithm.
     * 
     * The question is which data point to select from the 'following’ bucket. 
     * Ideally all data  points of the 'following’ bucket should be considered, 
     * but in order to reduce the search  space (and execution time) the algorithm 
     * calculates the average of the data points in the 'following’ bucket and uses 
     * that to determine the data point to select for the 'current’ bucket. 
     * 
     * This average data point is ignored as soon as the algorithm moves on to the 
     * next bucket.
     */
    
    var a:Int = 0
    var nextA:Int = 0
    
    /** FIRST BUCKET **/

    sampled += data(a)

    var maxAreaPoint = Point(x=0D, y=0D)
    
    (0 until sampleSize -2).foreach(i => {
      
         /** THIRD BUCKET **/

        /* Calculate point average for next bucket */
        val avgRangeStart = (Math.floor((i + 1) * bucketSize) + 1).toInt
        var avgRangeEnd = (Math.floor((i + 2) * bucketSize) + 1).toInt
        
        avgRangeEnd = if (avgRangeEnd < dataSize) avgRangeEnd else dataSize
        val avgRangeLength = avgRangeEnd - avgRangeStart
      
        var avgX = 0D
        var avgY = 0D

        (avgRangeStart until avgRangeEnd).foreach(j => {
            avgX += data(j).x
            avgY += data(j).y
        })

        avgX /= avgRangeLength
        avgY /= avgRangeLength
      
         /** SECOND BUCKET **/

        /* Get the range for this bucket */
        val rangeOffs = (Math.floor((i + 0) * bucketSize) + 1).toInt
        val rangeTo = (Math.floor((i + 1) * bucketSize) + 1).toInt

        /* Point a */
        val pointAx = data(a).x
        val pointAy = data(a).y

        var maxArea:Double = -1
        (rangeOffs until rangeTo).foreach(j => {
          
            /* Calculate triangle area over three buckets */
            val area = Math.abs(
                (pointAx - avgX) * (data(j).y - pointAy) - (pointAx - data(j).x) * (avgY - pointAy)
            ) * 0.5

            if (area > maxArea) {
              
                maxArea = area;
                maxAreaPoint = data(rangeOffs)
                nextA = rangeOffs
            
            }
        })

        /* Pick this point from the bucket */
        sampled += maxAreaPoint 
        
        /* This a is the next a (chosen b) */
        a = nextA
    })

    sampled += data(dataSize - 1)
    sampled.toArray
 
  }

  override def transformSchema(schema:StructType):StructType = {    
    schema    
  }

  override def copy(extra:ParamMap):LTTBuckets = defaultCopy(extra)
  
}