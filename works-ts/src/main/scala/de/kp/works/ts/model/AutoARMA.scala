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
import com.suning.spark.ts.{AutoARMA => SuningAutoARMA}
import com.suning.spark.ts.{ARMA => SuningARMA}

import com.suning.spark.regression.{LinearRegression => SuningRegression}

import org.apache.spark.ml.{Estimator,Model}

import org.apache.hadoop.fs.Path

import org.apache.spark.ml._
import org.apache.spark.ml.linalg.Vector

import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._

import org.apache.spark.ml.util._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

trait AutoARMAParams extends ModelParams 
      with HasPMaxParam with HasQMaxParam
      with HasRegParam with HasElasticNetParam 
      with HasStandardizationParam with HasFitInterceptParam 
      with HasCriterionParam {
  
}

class AutoARMA(override val uid: String)
  extends Estimator[AutoARMAModel] with AutoARMAParams with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("AutoARMA"))

  override def fit(dataset:Dataset[_]):AutoARMAModel = {

    require($(pmax) > 0 && $(qmax) > 0, s"Parameter pmax, qmax  must be positive")
 
    validateSchema(dataset.schema)
 
    val suning = SuningAutoARMA($(valueCol), $(timeCol), $(pmax), $(qmax),
      $(regParam), $(standardization), $(elasticNetParam), $(fitIntercept), $(criterion))
      
    val model = suning.fit(dataset.toDF)

    val p = model.getPBest
    val q = model.getQBest

    val intercept = model.getIntercept
    val weights = model.getWeights

    copyValues(new AutoARMAModel(uid, p, q, intercept, weights).setParent(this))

  }

  override def transformSchema(schema:StructType):StructType = {
    schema
  }

  override def copy(extra:ParamMap):AutoARMA = defaultCopy(extra)
  
}

class AutoARMAModel(override val uid:String, p:Int, q:Int, intercept:Double, weights:Vector)
  extends Model[AutoARMAModel] with AutoARMAParams with MLWritable {

  import AutoARMAModel._

  def this(p:Int, q:Int, intercept:Double, weights:Vector) = {
    this(Identifiable.randomUID("AutoARMAModel"), p, q, intercept, weights)
  }
  
  def getPBest:Int = p

  def getQBest:Int = q

  def getIntercept:Double = intercept
  
  def getWeights:Vector = weights
      
  def evaluate(predictions:Dataset[Row]):String = {
    /*
     * Reminder: AutoARMA is an ARMA model with
     * the best p & q parameters
     */
    val p = getPBest
    val q = getQBest
    
    val arma = SuningARMA($(valueCol), $(timeCol), p, q,
      $(regParam), $(standardization), $(elasticNetParam), $(fitIntercept))
 
		val labelCol = arma.getLabelCol
		val predictionCol = arma.getPredictionCol
				
	  Evaluator.evaluate(predictions, labelCol, predictionCol)
    
  }

  def forecast(dataset:Dataset[_], steps:Int):DataFrame = {
 
    validateSchema(dataset.schema)
    /*
     * Reminder: AutoARMA is an ARMA model with
     * the best p & q parameters
     */
    val p = getPBest
    val q = getQBest
    
    val arma = SuningARMA($(valueCol), $(timeCol), p, q,
      $(regParam), $(standardization), $(elasticNetParam), $(fitIntercept))
      
    val prepared = arma.prepareARMA(dataset.toDF)
    val featureCols = arma.getFeatureCols
    
    val intercept = getIntercept
    val weights = getWeights
    
    val model = LinearRegressionModel.get(intercept,weights)    

    val linearReg = new SuningRegression(featureCols)
    linearReg.setModel(model)
    
    val predictions = linearReg.transform(prepared).orderBy(desc($(timeCol)))
    arma.forecast(predictions, intercept, weights, steps)
    
  }
  
  override def transform(dataset:Dataset[_]):DataFrame = {
  
    validateSchema(dataset.schema)
   /*
     * Reminder: AutoARMA is an ARMA model with
     * the best p & q parameters
     */
    val p = getPBest
    val q = getQBest
    
    val arma = SuningARMA($(valueCol), $(timeCol), p, q,
      $(regParam), $(standardization), $(elasticNetParam), $(fitIntercept))
      
    val prepared = arma.prepareARMA(dataset.toDF)
    val featureCols = arma.getFeatureCols
    
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

  override def copy(extra:ParamMap):AutoARMAModel = {
    val copied = new AutoARMAModel(uid, p, q, intercept, weights).setParent(parent)
    copyValues(copied, extra)
  }

  override def write: MLWriter = new AutoARMAModelWriter(this)

}

object AutoARMAModel extends MLReadable[AutoARMAModel] {

  private case class Data(p:Int, q:Int, intercept: Double, coefficients: Vector)
  
  class AutoARMAModelWriter(instance: AutoARMAModel) extends MLWriter {

    override def save(path:String): Unit = {
      super.save(path)
    }
    
    override def saveImpl(path: String): Unit = {
      
      /* Save metadata & params */
      SparkParamsWriter.saveMetadata(instance, path, sc)
      
      val p = instance.getPBest
      val q = instance.getQBest
      /* 
       * Save intercept & weight of the underlying 
       * linear regression model
       */
      val intercept = instance.getIntercept
      val coefficients = instance.getWeights
      
      val data = Data(p, q, intercept, coefficients)
      val dataPath = new Path(path, "data").toString
      
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)      
            
    }
    
  }

  private class AutoARMAModelReader extends MLReader[AutoARMAModel] {

    private val className = classOf[AutoARMAModel].getName

    override def load(path: String):AutoARMAModel = {
      
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
      val model = new AutoARMAModel(metadata.uid, data.p, data.q, data.intercept, data.coefficients)
      SparkParamsReader.getAndSetParams(model, metadata)
      
      model
      
    }
  }

  override def read: MLReader[AutoARMAModel] = new AutoARMAModelReader

  override def load(path: String): AutoARMAModel = super.load(path)
  
}