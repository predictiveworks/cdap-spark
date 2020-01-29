package de.kp.works.ts.model
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

import com.suning.spark.ts.{ARIMA => SuningARIMA, ARMA => SuningARMA, DiffAutoRegression => SuningDiffAR}
import com.suning.spark.regression.{LinearRegression => SuningRegression}

import org.apache.hadoop.fs.Path

import org.apache.spark.ml._
import org.apache.spark.ml.linalg.Vector

import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._

import org.apache.spark.ml.util._

import org.apache.spark.sql._
import org.apache.spark.sql.types._

trait ARIMAParams extends ModelParams 
      with HasPParam with HasDParam with HasQParam 
      with HasRegParam with HasElasticNetParam 
      with HasStandardizationParam with HasFitInterceptParam
      with HasMeanOutParam {
  
  setDefault(standardization -> true, fitIntercept -> false)
  
}

class ARIMA(override val uid: String)
  extends Estimator[ARIMAModel] with ARIMAParams with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("arima"))

  override def fit(dataset: Dataset[_]): ARIMAModel = {

    require($(p) > 0 && $(d) > 0 && $(q) > 0, s"Parameters p, d, q must be positive")
 
    val suning = SuningARIMA(
      $(valueCol), $(timeCol), $(p), $(d), $(q),
      $(regParam), $(standardization), $(elasticNetParam), $(fitIntercept), $(meanOut))
    
    val model = suning.fit(dataset.toDF)

    val intercept = model.getIntercept
    val weights = model.getWeights

    copyValues(new ARIMAModel(uid, intercept, weights).setParent(this))

  }

  override def transformSchema(schema:StructType):StructType = {
    schema
  }

  override def copy(extra:ParamMap):ARIMA = defaultCopy(extra)
  
}

class ARIMAModel(override val uid:String, intercept:Double, weights:Vector)
  extends Model[ARIMAModel] with ARIMAParams with MLWritable {

  import ARIMAModel._

  def this(intercept:Double, weights:Vector) = {
    this(Identifiable.randomUID("arimaModel"), intercept, weights)
  }

  def getIntercept:Double = intercept
  
  def getWeights:Vector = weights
  
  override def transform(dataset:Dataset[_]):DataFrame = {

    val arima = SuningARIMA($(valueCol), $(timeCol), $(p), $(d), $(q),
      $(regParam), $(standardization), $(elasticNetParam), $(fitIntercept), $(meanOut))
    
    val prepared = arima.prepareARIMA(dataset.toDF)
    val featureCols = arima.getFeatureCols
    
    val intercept = getIntercept
    val weights = getWeights
    
    val model = LinearRegressionModel.get(intercept,weights)    

    val linearReg = new SuningRegression(featureCols)
    linearReg.setModel(model)
    
    linearReg.transform(prepared)

  }

  override def transformSchema(schema:StructType):StructType = {
    schema
  }

  override def copy(extra:ParamMap):ARIMAModel = {
    val copied = new ARIMAModel(uid, intercept, weights).setParent(parent)
    copyValues(copied, extra)
  }

  override def write: MLWriter = new ARIMAModelWriter(this)

}

object ARIMAModel extends MLReadable[ARIMAModel] {

  private case class Data(intercept: Double, coefficients: Vector)
  
  class ARIMAModelWriter(instance: ARIMAModel) extends MLWriter {

    override def save(path:String): Unit = {
      super.save(path)
    }

    override def saveImpl(path: String): Unit = {
      
      /* Save metadata & params */
      SparkParamsWriter.saveMetadata(instance, path, sc)
      
      /* 
       * Save intercept & weight of the underlying 
       * linear regression model
       */
      val intercept = instance.getIntercept
      val coefficients = instance.getWeights
      
      val data = Data(intercept, coefficients)
      val dataPath = new Path(path, "data").toString
      
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)      
      
    }
    
  }

  private class ARIMAModelReader extends MLReader[ARIMAModel] {

    private val className = classOf[ARIMAModel].getName

    override def load(path: String):ARIMAModel = {
      
      /* Read metadata & params */
      val metadata = SparkParamsReader.loadMetadata(path, sc, className)
      /*
       * Retrieve intercept & weight of the underlying
       * linear regression model
       */
      val sparkSession = super.sparkSession
      import sparkSession.implicits._

      val dataPath = new Path(path, "data").toString
      val data: Data =  sparkSession.read.parquet(dataPath).as[Data].collect.head
      /*
       * Reconstruct trained model instance
       */
      val model = new ARIMAModel(metadata.uid, data.intercept, data.coefficients)
      SparkParamsReader.getAndSetParams(model, metadata)
      
      model
      
    }
  }

  override def read: MLReader[ARIMAModel] = new ARIMAModelReader

  override def load(path: String): ARIMAModel = super.load(path)
  
}