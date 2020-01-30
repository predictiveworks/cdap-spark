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

import com.suning.spark.ts.{AutoARIMA => SuningAutoARIMA}
import com.suning.spark.ts.{ARIMA => SuningARIMA}
import com.suning.spark.regression.{LinearRegression => SuningRegression}

import org.apache.spark.ml.{Estimator,Model}

import org.apache.hadoop.fs.Path

import org.apache.spark.ml._
import org.apache.spark.ml.linalg.Vector

import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._

import org.apache.spark.ml.util._

import org.apache.spark.sql._
import org.apache.spark.sql.types._

trait AutoARIMAParams extends ModelParams 
      with HasPMaxParam with HasDMaxParam with HasQMaxParam 
      with HasRegParam with HasElasticNetParam 
      with HasStandardizationParam with HasFitInterceptParam
      with HasMeanOutParam with HasCriterionParam {
  
}

class AutoARIMA(override val uid: String)
  extends Estimator[AutoARIMAModel] with AutoARIMAParams with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("AutoARIMA"))

  override def fit(dataset:Dataset[_]):AutoARIMAModel = {

    require($(pmax) > 0 && $(dmax) > 0 && $(qmax) > 0, s"Parameter pmax, dmax, qmax  must be positive")
 
    val suning = SuningAutoARIMA($(valueCol), $(timeCol), $(pmax), $(dmax), $(qmax),
      $(regParam), $(standardization), $(elasticNetParam), $(fitIntercept), $(meanOut), $(criterion))
      
    val model = suning.fit(dataset.toDF)

    val p = model.getPBest
    val d = model.getDBest
    val q = model.getQBest

    val intercept = model.getIntercept
    val weights = model.getWeights

    copyValues(new AutoARIMAModel(uid, p, d, q, intercept, weights).setParent(this))
    
  }

  override def transformSchema(schema:StructType):StructType = {
    schema
  }

  override def copy(extra:ParamMap):AutoARIMA = defaultCopy(extra)
  
}

class AutoARIMAModel(override val uid:String, p:Int, d:Int, q:Int, intercept:Double, weights:Vector)
  extends Model[AutoARIMAModel] with AutoARIMAParams with MLWritable {

  import AutoARIMAModel._

  def this(p:Int, d:Int, q:Int, intercept:Double, weights:Vector) = {
    this(Identifiable.randomUID("AutoARIMAModel"), p, d, q, intercept, weights)
  }
  
  def getPBest:Int = p
  
  def getDBest:Int = d

  def getQBest:Int = q

  def getIntercept:Double = intercept
  
  def getWeights:Vector = weights
      
  def evaluate(predictions:Dataset[Row]):String = {
    /*
     * Reminder: AutoARIMA is an ARIMA model with
     * the best p, d & q parameters
     */
    val p = getPBest
    val d = getDBest
    val q = getQBest
    
    val arima = SuningARIMA($(valueCol), $(timeCol), p, d, q,
      $(regParam), $(standardization), $(elasticNetParam), $(fitIntercept), $(meanOut))
 
		val labelCol = arima.getLabelCol
		val predictionCol = arima.getPredictionCol
				
	  Evaluator.evaluate(predictions, labelCol, predictionCol)
    
  }
  
  override def transform(dataset:Dataset[_]):DataFrame = {
    /*
     * Reminder: AutoARIMA is an ARIMA model with
     * the best p, d & q parameters
     */
    val p = getPBest
    val d = getDBest
    val q = getQBest
    
    val arima = SuningARIMA($(valueCol), $(timeCol), p, d, q,
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

  override def copy(extra:ParamMap):AutoARIMAModel = {
    val copied = new AutoARIMAModel(uid, p, d, q, intercept, weights).setParent(parent)
    copyValues(copied, extra)
  }

  override def write: MLWriter = new AutoARIMAModelWriter(this)

}

object AutoARIMAModel extends MLReadable[AutoARIMAModel] {

  private case class Data(p:Int, d:Int, q:Int, intercept: Double, coefficients: Vector)
  
  class AutoARIMAModelWriter(instance: AutoARIMAModel) extends MLWriter {

    override def save(path:String): Unit = {
      super.save(path)
    }
    
    override def saveImpl(path: String): Unit = {
      
      /* Save metadata & params */
      SparkParamsWriter.saveMetadata(instance, path, sc)
      
      val p = instance.getPBest
      val d = instance.getDBest
      val q = instance.getQBest
      /* 
       * Save intercept & weight of the underlying 
       * linear regression model
       */
      val intercept = instance.getIntercept
      val coefficients = instance.getWeights
      
      val data = Data(p, d, q, intercept, coefficients)
      val dataPath = new Path(path, "data").toString
      
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)                  
      
    }
    
  }

  private class AutoARIMAModelReader extends MLReader[AutoARIMAModel] {

    private val className = classOf[AutoARIMAModel].getName

    override def load(path: String):AutoARIMAModel = {
      
      /* Read metadata & params */
      val metadata = SparkParamsReader.loadMetadata(path, sc, className)
      /*
       * Retrieve q, and intercept & weight of the underlying
       * linear regression model
       */
      val sparkSession = super.sparkSession
      import sparkSession.implicits._

      val dataPath = new Path(path, "data").toString
      val data: Data =  sparkSession.read.parquet(dataPath).as[Data].collect.head
      /*
       * Reconstruct trained model instance
       */
      val model = new AutoARIMAModel(metadata.uid, data.p, data.d, data.q, data.intercept, data.coefficients)
      SparkParamsReader.getAndSetParams(model, metadata)
      
      model
      
    }
  }

  override def read: MLReader[AutoARIMAModel] = new AutoARIMAModelReader

  override def load(path: String): AutoARIMAModel = super.load(path)
  
}