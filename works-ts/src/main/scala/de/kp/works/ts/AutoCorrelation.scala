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
import org.apache.hadoop.fs.Path

import org.apache.spark.ml.{Estimator,Model, ParamsIO}

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
      "The maximum lag value. Default is 1.", (value:Int) => true)

  final val lagValues = new Param[Array[Int]](AutoCorrelationParams.this, "lagValues",
      "The sequence of lag value. This sequence is empty by default.", (value:Array[Int]) => true)

  final val threshold = new Param[Double](AutoCorrelationParams.this, "threshold",
      "The threshold used to determine the lag value with the highest correlation score. "
      + "Default is 0.95.", (value:Double) => true)    
      
  /** @group setParam */
  def setValueCol(value:String): AutoCorrelationParams.this.type = set(valueCol, value)

  /** @group setParam */
  def setMaxLag(value:Int): AutoCorrelationParams.this.type = set(maxLag, value)

  /** @group setParam */
  def setLagValues(value:Array[Int]): AutoCorrelationParams.this.type = set(lagValues, value)

  /** @group setParam */
  def setThreshold(value:Double): AutoCorrelationParams.this.type = set(threshold, value)
      
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
  
  setDefault(maxLag -> -1, lagValues -> Array.empty[Int], threshold -> 0.95)
  
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
 * Lags that result in high correlation will have values close to 1 or -1, 
 * for positive and negative correlations. 
 */
class AutoCorrelation(override val uid: String)
  extends Estimator[AutoCorrelationModel] with AutoCorrelationParams with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("autoCorrelation"))

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

    if ($(maxLag) == -1 && $(lagValues).isEmpty)
      throw new IllegalArgumentException("[AutoCorrelation] No lag values specified.")

    val values = if ($(maxLag) > 0) {      
      (1 to $(maxLag)).map(k => compute(dataset, average, denom, k)).toArray
    
    } else
      $(lagValues).map(k => compute(dataset, average, denom, k))
    
    /* Build AutoCorrelationModel */
    copyValues(new AutoCorrelationModel(uid, average, denom, values).setParent(this))
    
  }
  /*
   * This method computes the auto correlation value for 
   * a certain lag value k
   */
  private def compute(dataset:Dataset[_], average:Double, denom: Double, k:Int):Double = {
    
    /* Specify a window with k + 1 values */
    val spec = Window.partitionBy().rowsBetween(0, k)
    val windowed = dataset.withColumn("_vector", collect_list(col($(valueCol)).cast(DoubleType)).over(spec))
    
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

class AutoCorrelationModel(override val uid:String, val average:Double, val denom: Double, val values:Seq[Double])
  extends Model[AutoCorrelationModel] with AutoCorrelationParams with MLWritable {

  import AutoCorrelationModel._

  def this(average:Double, denom:Double, values: Seq[Double]) = {
    this(Identifiable.randomUID("autoCorrelationModel"),average, denom, values)
  }
  /**
   * This method returns the lag values with the highest 
   * correlation factor if above threshold; otherwise -1 
   */
  def getSeasonalPeriod():Int = {
    
    /* 
     * Zip withing index, sort and determine 
     * highest correlation factor and its index
     */
    val max = values.zipWithIndex.sortBy(-_._1).head
    /*
     * Note, the respective index = lag - 1
     */
    if (max._1 >= $(threshold)) (max._2 + 1) else -1
    
  }
  /*
   * The model does not transform any dataset
   */
  override def transform(dataset:Dataset[_]):DataFrame = {
    dataset.toDF
  }

  override def transformSchema(schema:StructType):StructType = {
    schema
  }

  override def copy(extra:ParamMap):AutoCorrelationModel = {
    val copied = new AutoCorrelationModel(uid, average, denom, values).setParent(parent)
    copyValues(copied, extra)
  }

  override def write: MLWriter = new AutoCorrelationModelWriter(this)
  
}

object AutoCorrelationModel extends MLReadable[AutoCorrelationModel] {

  /** Helper class for storing model data */
  private case class Data(average: Double, denom: Double, values: Array[Double])
  
  class AutoCorrelationModelWriter(instance: AutoCorrelationModel) extends MLWriter {

    override def save(path:String): Unit = {
      super.save(path)
    }
    
    override def saveImpl(path: String): Unit = {

      /* Save metadata and params */
      ParamsIO.saveMetadata(instance, path, sc)
      
      /* Save model data: average, denom, values */
      val data:Array[Data] = Array(Data(instance.average, instance.denom, instance.values.toArray))
     
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(data).repartition(1).write.parquet(dataPath)
      
    }
    
  }

  private class AutoCorrelationModelReader extends MLReader[AutoCorrelationModel] {

    private val className = classOf[AutoCorrelationModel].getName

    override def load(path: String):AutoCorrelationModel = {

      val sparkSession = super.sparkSession
      import sparkSession.implicits._

      val metadata = ParamsIO.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString

      val data: Data =  sparkSession.read.parquet(dataPath).as[Data].collect.head
      
      val model = new AutoCorrelationModel(metadata.uid, data.average, data.denom, data.values)
      ParamsIO.getAndSetParams(model, metadata)

      model
      
    }
  }

  override def read: MLReader[AutoCorrelationModel] = new AutoCorrelationModelReader

  override def load(path: String): AutoCorrelationModel = super.load(path)
  
}