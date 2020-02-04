package de.kp.works.ml.smote
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

import org.apache.spark.ml.feature.{VectorAssembler, VectorFeaturizer}

import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.util._

import org.apache.spark.sql._

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.mutable.WrappedArray

trait SmoteBalancerParams extends Params {
  
  final val featuresCol = new Param[String](SmoteBalancerParams.this, "featuresCol",
      "Name of the features field", (value:String) => true)
  
  final val labelCol = new Param[String](SmoteBalancerParams.this, "labelCol",
      "Name of the label field", (value:String) => true)
 
  def setFeaturesCol(value:String): this.type = set(featuresCol, value)
 
  def setLabelCol(value:String): this.type = set(labelCol, value)
  /**
   * The length of each hash bucket, a larger bucket lowers the false negative rate. The number of
   * buckets will be `(max L2 norm of input vectors) / bucketLength`.
   *
   *
   * If input vectors are normalized, 1-10 times of pow(numRecords, -1/inputDim) would be a
   * reasonable value
   */
  val bucketLength: DoubleParam = new DoubleParam(this, "bucketLength",
    "the length of each hash bucket, a larger bucket lowers the false negative rate.",
    ParamValidators.gt(0))

  def setBucketLength(value:Double): this.type = set(bucketLength, value)
  /**
   * Param for the number of hash tables used in LSH OR-amplification.
   *
   * LSH OR-amplification can be used to reduce the false negative rate. Higher values for this
   * param lead to a reduced false negative rate, at the expense of added computational complexity.
   */
  final val numHashTables: IntParam = new IntParam(this, "numHashTables", "number of hash " +
    "tables, where increasing number of hash tables lowers the false negative rate, and " +
    "decreasing it improves the running performance", ParamValidators.gt(0))

  def setNumHashTables(value:Int): this.type = set(numHashTables, value)

  final val numNearestNeighbors: IntParam = new IntParam(this, "numNearestNeighbors", 
      "number of nearest neighbours must be greater than or equals 1.", ParamValidators.gt(0))

  def setNumNearestNeighbors(value:Int): this.type = set(numNearestNeighbors, value)
  
  setDefault(bucketLength -> 2.0, numHashTables -> 1, numNearestNeighbors -> 4)
  
  def validateSchema(schema:StructType):Unit = {
    
    /** FEATURES **/
    
    val featuresColName = $(featuresCol)  
    
    if (schema.fieldNames.contains(featuresColName) == false)
      throw new IllegalArgumentException(s"Features column $featuresColName does not exist.")
    
    /** LABELS **/
    
    val labelColName = $(labelCol)  
    
    if (schema.fieldNames.contains(labelColName) == false)
      throw new IllegalArgumentException(s"Label column $labelColName does not exist.")
    
    
  }

}

class SmoteBalancer(override val uid: String) extends Transformer with SmoteBalancerParams {
    
  def this() = this(Identifiable.randomUID("smoteBalancer"))

  /*
   * This helpe method computes those labels of the 
   * incoming dataset that are subject to over-sampling:
   * 
   * The SMOTE multipler must be greater than one
   */
  private def computeLabelDist(dataset:Dataset[_]):Array[(Double,Double,Int)] = {
    /*
     * Determine label distribution within the dataset
     */
    val counts = dataset
      .withColumn("_label", col($(labelCol)).cast(DoubleType))
      .groupBy("_label").agg(count("*").as("count")).collect
      .map{case Row(label:Double, count: Long) => (label,count)}
    
    /*
     * Evaluate label distribution and determine which
     * labels have to be balanced by SMOTE algorithm
     */
    val max = counts.map(v => v._2).max
    if (max == 0L) return Array.empty[(Double,Double,Int)]

    counts.map(v => {
      val label = v._1
      val size  = v._2
      /*
       * Compute fraction to be able to apply the provided
       * threshold to filter those minor subsets that have
       * to be recognized
       */
      val fraction = v._2.toDouble / max
      /*
       * The SMOTE algorithm requires a multiplier that
       * defines the number of synthetic examples to create, 
       * per example in the sample
       * 
       * max = size * (multiplier + 1)
       */
      val multiplier = Math.floor(max / size - 1).toInt
      
      (label, fraction, multiplier)
      }).filter(v => v._3 > 1)
    
    
  }
  def transform(dataset:Dataset[_]):DataFrame = {
    
    validateSchema(dataset.schema)
    
    val sampleset = dataset.select($(labelCol), $(featuresCol))
    /*
     * STEP #1: Determine label distribution within the dataset
     * and determine which labels have to be balanced by SMOTE 
     * algorithm
     */
    val dist = computeLabelDist(sampleset)
    if (dist.isEmpty) return dataset.toDF
    /*
     * STEP #2: Split dataset into major and minor datasets
     */
    val minorLabels = dist.map(_._1).toList
    val minorFilter = minorFilter_udf(minorLabels)

    val majorset = sampleset.filter(minorFilter(col($(labelCol))) === false)
    val minorset = sampleset.filter(minorFilter(col($(labelCol))) === true)
    /*
     * STEP #4: Feature each minor dataset and apply SMOTE
     * algorithm to resample with synthetic data rows.
     * 
     * The SMOTE algorithm requires a dissembled representation
     * of the features, which results in additional columns like
     * f_0, f_1, f_2, ...
     */
    val featurizer = new VectorFeaturizer().setFeaturesCol($(featuresCol))
    val assembler = new VectorAssembler().setOutputCol($(featuresCol))

    val synthetics = dist.map{case(label, fraction, multiplier) => {
      /*
       * Select a subset of the 'minor' dataset that refers to
       * a certain minor label
       */
      val featurized = featurizer.transform(minorset.filter(col($(labelCol)) === label))
      val featureCols = featurized.schema.fieldNames.filter(fname => fname.startsWith("f_")) 
      /*
       * Apply SMOTE algorithm and compute additional synthetic
       * data records
       */
      val smote = Smote(
          featurized, 
          $(featuresCol), 
          featureCols, 
          Some($(bucketLength)),
          $(numHashTables),
          multiplier,
          $(numNearestNeighbors))(featurized.sparkSession)
      val synthetic = smote.syntheticSample.withColumn($(labelCol), lit(label))
      /*
       * The result of the SMOTE algorithm comprises the label 
       * and the featureCols
       */
      assembler
        .setInputCols(featureCols)
        .transform(synthetic)
        .drop(featureCols: _*)
    }}
    /*
     * The result of the SMOTE algorithm comprises the label 
     * and the featureCols
     */
    val syntheticset = synthetics.reduce(_ union _)
    sampleset.union(syntheticset)

  }
  
  def minorFilter_udf(labels:List[Double]) = udf{label:Double => {
    if (labels.contains(label)) true else false
  }}
  
  override def transformSchema(schema:StructType):StructType = {    
    schema    
  }

  override def copy(extra:ParamMap):SmoteBalancer = defaultCopy(extra)
  
}