package de.kp.works.ts
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
import org.apache.spark.SparkException

import org.apache.spark.ml.{Estimator,Model}

import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._

import org.apache.spark.ml.util._

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.mutable.WrappedArray

trait AutoCorrelationParams extends Params {

  final val valueCol = new Param[String](AutoCorrelationParams.this, "valueCol",
      "Name of the value field", (value:String) => true)

  /*
   * This auto correlation function computes values for a sequence of 
   * lag values either specified by its max lag, i.e. 1 <= k <= max_lag,
   * or, by a discrete list of lag values
   */
  final val maxLag = new Param[Int](AutoCorrelationParams.this, "maxLag",
      "The maximum lag value", (value:Int) => true)

  final val lagValues = new Param[Seq[Int]](AutoCorrelationParams.this, "lagValues",
      "The sequence of lag value", (value:Seq[Int]) => true)
      
  def validateSchema(schema:StructType):Unit = {
    
    /* VALUE FIELD */
    
    val valueColName = $(valueCol)  
    
    if (schema.fieldNames.contains(valueColName) == false)
      throw new IllegalArgumentException(s"Value column $valueColName does not exist.")
    
    val valueColType = schema(valueColName).dataType
    if (valueColType != DoubleType) {
      throw new IllegalArgumentException(s"Data type of value column $valueColName must be a Double.")
    }

  }
  
  setDefault(maxLag -> -1, lagValues -> Seq.empty[Int])
  
}
/**
 * [AutoCorrelation] expects a dataset that contains dataset at least with
 * a single column of Double values. The auto correlation function is useful
 * e.g. to check whether a (univariate) time series is stationary or not.
 * 
 * The auto correlation function is either computed for range of lag values
 * that is specified by its maximum value, or, by a sequence of discrete
 * lag values.
 * 
 * Lags that result in high correlation will have values colse to 1 or -1, 
 * for positive and negative correlations. 
 */
class AutoCorrelation(override val uid: String)
  extends Estimator[AutoCorrelationModel] with AutoCorrelationParams with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("autoCorrelation"))

  /** @group setParam */
  def setValueCol(value:String): AutoCorrelation.this.type = set(valueCol, value)

  /** @group setParam */
  def setMaxLag(value:Int): AutoCorrelation.this.type = set(maxLag, value)

  /** @group setParam */
  def setLagValues(value:Seq[Int]): AutoCorrelation.this.type = set(lagValues, value)

  override def fit(dataset:Dataset[_]):AutoCorrelationModel = {

    /* 
     * Make sure that the value column exists and the respective
     * data type is Double
     */
    validateSchema(dataset.schema)

    /* Compute average value of all values of the valueCol */
    val average = dataset.agg({$(valueCol) -> "avg"}).collect.head(0).asInstanceOf[Double]

    /* Compute the auto correlation demoninator */
    
    val variance = variance_udf(average)
    val variances = dataset.withColumn("_variance", variance(col($(valueCol))))
     
    val denom = variances.agg({"_variance" -> "sum"}).collect.head(0).asInstanceOf[Double]
    
    /* Build AutoCorrelationModel */
    copyValues(new AutoCorrelationModel(uid,average, denom).setParent(this))
    
  }
  /*
   * This method is a helper function to compute the denominator
   * for the auto correlation function
   */
  private def variance_udf(average:Double) = udf {
    (value: Double) => {
      (value - average) * (value - average)
    }
  }

  override def transformSchema(schema:StructType):StructType = {
    schema
  }

  override def copy(extra:ParamMap):AutoCorrelation = defaultCopy(extra)
  
}


class AutoCorrelationModel(override val uid:String, val average:Double, val denom: Double)
  extends Model[AutoCorrelationModel] with AutoCorrelationParams with MLWritable {

  import AutoCorrelationModel._

  def this(average:Double, denom:Double) = {
    this(Identifiable.randomUID("autoCorrelationModel"),average, denom)
  }

  /** @group setParam */
  def setValueCol(value: String): this.type = set(valueCol, value)

  /**
   * This method computes the auto correlation function for a 
   * univariate dataset (timeseries)
   */
  override def transform(dataset:Dataset[_]):DataFrame = {

    if ($(maxLag) == -1 && $(lagValues).isEmpty)
      throw new IllegalArgumentException("[AutoCorrelation] No lag values specified.")

    val values = if ($(maxLag) > 0) {      
      (1 to $(maxLag)).map(k => compute(dataset, k))
    
    } else
      $(lagValues).map(k => compute(dataset, k))


    import dataset.sparkSession.implicits._
    val result = dataset.sparkSession.createDataset[Double](values).toDF("values")
    
    result

  }
  /*
   * This method computes the auto correlation value for 
   * a certain lag value k
   */
  private def compute(dataset:Dataset[_], k:Int):Double = {
    
    /* Specify a window with k + 1 values */
    val spec = Window.partitionBy().rowsBetween(0, k)
    val windowed = dataset.withColumn("_vector", collect_list(col($(valueCol))).over(spec))
    
    /* Instance of the correlation function with 
     * a specific k and average value
     */
    val correlate = correlate_udf(k, average)

    val corrVal = windowed
      /* Correlate vector values of distance k */
      .withColumn("_corr", correlate(col("_vector")))
      /* Compute sum of correlation values */
      .agg({"_corr" -> "sum"}).collect.head(0).asInstanceOf[Double]
    
    if (denom == 0D) 0D else corrVal / denom
    
  }
  
  private def correlate_udf(k:Int, average:Double) = udf {
    vector:WrappedArray[Double] => {
      /* 
       * We expect a vector of length k + 1; for smaller ones,
       * i.e. near the upper end of the values, the correlation
       * value is set to zero
       */
      if (vector.length == k + 1) {
        (vector(0) - average) * (vector(k) - average)
        
      } else 0D

    }
  }

  override def transformSchema(schema:StructType):StructType = {
    schema
  }

  override def copy(extra:ParamMap):AutoCorrelationModel = {
    val copied = new AutoCorrelationModel(uid,average,denom).setParent(parent)
    copyValues(copied, extra)
  }

  override def write: MLWriter = new AutoCorrelationModelWriter(this)
  
}

object AutoCorrelationModel extends MLReadable[AutoCorrelationModel] {

  class AutoCorrelationModelWriter(instance: AutoCorrelationModel) extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      throw new SparkException("Method 'saveImpl' is not implemented")
    }
    
  }

  private class AutoCorrelationModelReader extends MLReader[AutoCorrelationModel] {

    private val className = classOf[AutoCorrelationModel].getName

    override def load(path: String):AutoCorrelationModel = {
      throw new SparkException("Method 'load' is not supported")
    }
  }

  override def read: MLReader[AutoCorrelationModel] = new AutoCorrelationModelReader

  override def load(path: String): AutoCorrelationModel = super.load(path)
  
}