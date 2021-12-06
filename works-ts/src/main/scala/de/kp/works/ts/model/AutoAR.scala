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
import com.suning.spark.ts.{AutoAR => SuningAutoAR}
import com.suning.spark.ts.{AutoRegression => SuningAutoRegression}
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

trait AutoARParams extends ModelParams
      with HasPMaxParam 
      with HasRegParam with HasElasticNetParam
      with HasStandardizationParam with HasFitInterceptParam
      with HasMeanOutParam with HasEarlyStopParam with HasCriterionParam {
  
}

class AutoAR(override val uid: String)
  extends Estimator[AutoARModel] with AutoARParams with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("AutoAR"))

  override def fit(dataset:Dataset[_]):AutoARModel = {

    require($(pmax) > 0, s"Parameter pmax  must be positive")
 
    validateSchema(dataset.schema)
 
    val suning = SuningAutoAR($(valueCol), $(timeCol), $(pmax),
      $(regParam), $(standardization), $(elasticNetParam), $(fitIntercept), $(meanOut), $(criterion), $(earlyStop))
     
    val model = suning.fit(dataset.toDF)

    val p = model.getPBest
    val intercept = model.getIntercept
    val weights = model.getWeights

    copyValues(new AutoARModel(uid, p, intercept, weights).setParent(this))

  }

  override def transformSchema(schema:StructType):StructType = {
    schema
  }

  override def copy(extra:ParamMap):AutoAR = defaultCopy(extra)
  
}

class AutoARModel(override val uid:String, p:Int, intercept:Double, weights:Vector)
  extends Model[AutoARModel] with AutoARParams with MLWritable {

  import AutoARModel._

  def this(p:Int, intercept:Double, weights:Vector) = {
    this(Identifiable.randomUID("AutoARModel"), p, intercept, weights)
  }

  def getP:Int = p
  def getIntercept:Double = intercept
  
  def getWeights:Vector = weights
  
  def evaluate(predictions:Dataset[Row]):String = {
    /*
     * Reminder: AutoAR is an AutoRegression model with
     * the best p parameter
     */
    val p = getP
    
    val ar = SuningAutoRegression($(valueCol), $(timeCol), p,
      $(regParam), $(standardization), $(elasticNetParam), $(fitIntercept), $(meanOut))
 
		val labelCol = ar.getLabelCol
		val predictionCol = ar.getPredictionCol
				
	  RegressorEvaluator.evaluate(predictions, labelCol, predictionCol)
    
  }

  def forecast(dataset:Dataset[_], steps:Int):DataFrame = {
 
    validateSchema(dataset.schema)
    /*
     * Reminder: AutoAR is an AutoRegression model with
     * the best p parameter
     */
    val p = getP
    
    val ar = SuningAutoRegression($(valueCol), $(timeCol), p,
      $(regParam), $(standardization), $(elasticNetParam), $(fitIntercept), $(meanOut))
    
    val prepared = ar.prepareAR(dataset.toDF)
    val featureCols = ar.getFeatureCols
    
    val intercept = getIntercept
    val weights = getWeights
    
    val model = LinearRegressionModel.get(intercept,weights)    

    val linearReg = new SuningRegression(featureCols)
    linearReg.setModel(model)
    
    val meanValue = getDouble(dataset.select(mean($(valueCol))).collect()(0).get(0))    

    val predictions = linearReg.transform(prepared).orderBy(desc($(timeCol)))    
    ar.forecast(predictions, intercept, weights, meanValue, steps)
    
  }
  
  override def transform(dataset:Dataset[_]):DataFrame = {
 
    validateSchema(dataset.schema)
    /*
     * Reminder: AutoAR is an AutoRegression model with
     * the best p parameter
     */
    val p = getP
    
    val ar = SuningAutoRegression($(valueCol), $(timeCol), p,
      $(regParam), $(standardization), $(elasticNetParam), $(fitIntercept), $(meanOut))
    
    val prepared = ar.prepareAR(dataset.toDF)
    val featureCols = ar.getFeatureCols
    
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

  override def copy(extra:ParamMap):AutoARModel = {
    val copied = new AutoARModel(uid, p, intercept, weights).setParent(parent)
    copyValues(copied, extra)
  }

  override def write: MLWriter = new AutoARModelWriter(this)

}

object AutoARModel extends MLReadable[AutoARModel] {

  private case class Data(p:Int, intercept: Double, coefficients: Vector)
  
  class AutoARModelWriter(instance: AutoARModel) extends MLWriter {

    override def save(path:String): Unit = {
      super.save(path)
    }
    
    override def saveImpl(path: String): Unit = {
      
      /* Save metadata & params */
      SparkParamsWriter.saveMetadata(instance, path, sc)
      
      val p = instance.getP
      /* 
       * Save intercept & weight of the underlying 
       * linear regression model
       */
      val intercept = instance.getIntercept
      val coefficients = instance.getWeights
      
      val data = Data(p, intercept, coefficients)
      val dataPath = new Path(path, "data").toString
      
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)      
      
    }
    
  }

  private class AutoARModelReader extends MLReader[AutoARModel] {

    private val className = classOf[AutoARModel].getName

    override def load(path: String):AutoARModel = {
      
      /* Read metadata & params */
      val metadata = SparkParamsReader.loadMetadata(path, sc, className)
      /*
       * Retrieve p, and intercept & weight of the underlying
       * linear regression model
       */
      val sparkSession = super.sparkSession
      import sparkSession.implicits._

      val dataPath = new Path(path, "data").toString
      val data: Data =  sparkSession.read.parquet(dataPath).as[Data].collect.head
      /*
       * Reconstruct trained model instance
       */
      val model = new AutoARModel(metadata.uid, data.p, data.intercept, data.coefficients)
      SparkParamsReader.getAndSetParams(model, metadata)
      
      model
      
    }
  }

  override def read: MLReader[AutoARModel] = new AutoARModelReader

  override def load(path: String): AutoARModel = super.load(path)
  
}