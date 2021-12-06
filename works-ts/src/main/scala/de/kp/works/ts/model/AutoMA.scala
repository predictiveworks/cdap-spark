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
import com.suning.spark.ts.{AutoMA => SuningAutoMA}
import com.suning.spark.ts.{MovingAverage => SuningMovingAverage}
import com.suning.spark.regression.{LinearRegression => SuningRegression}
import de.kp.works.core.ml.RegressorEvaluator
import org.apache.spark.ml.{Estimator, Model}
import org.apache.hadoop.fs.Path
import org.apache.spark.ml._
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

trait AutoMAParams extends ModelParams with HasQMaxParam 
      with HasRegParam with HasElasticNetParam 
      with HasStandardizationParam with HasFitInterceptParam 
      with HasMeanOutParam with HasEarlyStopParam with HasCriterionParam {
  
}

class AutoMA(override val uid: String)
  extends Estimator[AutoMAModel] with AutoMAParams with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("AutoMA"))

  override def fit(dataset:Dataset[_]):AutoMAModel = {

    require($(qmax) > 0, s"Parameter qmax  must be positive")
 
    validateSchema(dataset.schema)
 
    val suning = SuningAutoMA($(valueCol), $(timeCol), $(qmax),
      $(regParam), $(standardization), $(elasticNetParam), $(fitIntercept), $(meanOut), $(criterion), $(earlyStop))
      
    val model = suning.fit(dataset.toDF)

    val q = model.getQBest
    val intercept = model.getIntercept
    val weights = model.getWeights

    copyValues(new AutoMAModel(uid, q, intercept, weights).setParent(this))

  }

  override def transformSchema(schema:StructType):StructType = {
    schema
  }

  override def copy(extra:ParamMap):AutoMA = defaultCopy(extra)
  
}

class AutoMAModel(override val uid:String, q:Int, intercept:Double, weights:Vector)
  extends Model[AutoMAModel] with AutoMAParams with MLWritable {

  import AutoMAModel._

  def this(q:Int, intercept:Double, weights:Vector) = {
    this(Identifiable.randomUID("AutoMAModel"), q, intercept, weights)
  }

  def getQBest:Int = q
  
  def getIntercept:Double = intercept
  
  def getWeights:Vector = weights
  
  def evaluate(predictions:Dataset[Row]):String = {
    /*
     * Reminder: AutoMA is an MovingAverage model with
     * the best q parameter
     */
    val q = getQBest
    
    val ma = SuningMovingAverage($(valueCol), $(timeCol), q,
      $(regParam), $(standardization), $(elasticNetParam), $(fitIntercept), $(meanOut))
 
		val labelCol = ma.getLabelCol
		val predictionCol = ma.getPredictionCol
				
	  RegressorEvaluator.evaluate(predictions, labelCol, predictionCol)
    
  }

  def forecast(dataset:Dataset[_], steps:Int):DataFrame = {
 
    validateSchema(dataset.schema)
    /*
     * Reminder: AutoMA is an MovingAverage model with
     * the best q parameter
     */
    val q = getQBest
    
    val ma = SuningMovingAverage($(valueCol), $(timeCol), q,
      $(regParam), $(standardization), $(elasticNetParam), $(fitIntercept), $(meanOut))
    
    val prepared = ma.prepareMA(dataset.toDF)
    val featureCols = ma.getFeatureCols
    
    val intercept = getIntercept
    val weights = getWeights
    
    val model = LinearRegressionModel.get(intercept,weights)    

    val linearReg = new SuningRegression(featureCols)
    linearReg.setModel(model)
    
    val predictions = linearReg.transform(prepared).orderBy(desc($(timeCol)))    
    ma.forecast(predictions, intercept, weights, steps)
    
  }
  
  override def transform(dataset:Dataset[_]):DataFrame = {
 
    validateSchema(dataset.schema)
    /*
     * Reminder: AutoMA is an MovingAverage model with
     * the best q parameter
     */
    val q = getQBest
    
    val ma = SuningMovingAverage($(valueCol), $(timeCol), q,
      $(regParam), $(standardization), $(elasticNetParam), $(fitIntercept), $(meanOut))
    
    val prepared = ma.prepareMA(dataset.toDF)
    val featureCols = ma.getFeatureCols
    
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

  override def copy(extra:ParamMap):AutoMAModel = {
    val copied = new AutoMAModel(uid, q, intercept, weights).setParent(parent)
    copyValues(copied, extra)
  }

  override def write: MLWriter = new AutoMAModelWriter(this)

}

object AutoMAModel extends MLReadable[AutoMAModel] {

  private case class Data(q:Int, intercept: Double, coefficients: Vector)
  
  class AutoMAModelWriter(instance: AutoMAModel) extends MLWriter {

    override def save(path:String): Unit = {
      super.save(path)
    }
    
    override def saveImpl(path: String): Unit = {
      
      /* Save metadata & params */
      SparkParamsWriter.saveMetadata(instance, path, sc)
      
      val q = instance.getQBest
      /* 
       * Save intercept & weight of the underlying 
       * linear regression model
       */
      val intercept = instance.getIntercept
      val coefficients = instance.getWeights
      
      val data = Data(q, intercept, coefficients)
      val dataPath = new Path(path, "data").toString
      
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)      
      
    }
    
  }

  private class AutoMAModelReader extends MLReader[AutoMAModel] {

    private val className = classOf[AutoMAModel].getName

    override def load(path: String):AutoMAModel = {
      
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
      val model = new AutoMAModel(metadata.uid, data.q, data.intercept, data.coefficients)
      SparkParamsReader.getAndSetParams(model, metadata)
      
      model
      
    }
  }

  override def read: MLReader[AutoMAModel] = new AutoMAModelReader

  override def load(path: String): AutoMAModel = super.load(path)
  
}