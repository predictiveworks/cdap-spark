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

import com.suning.spark.ts.{DiffAutoRegression => SuningDiffAutoRegression}
import com.suning.spark.regression.{LinearRegression => SuningRegression}

import org.apache.hadoop.fs.Path

import org.apache.spark.ml._
import org.apache.spark.ml.linalg.Vector

import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._

import org.apache.spark.ml.util._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

trait DiffAutoRegressionParams extends ModelParams with HasPParam with HasDParam 
      with HasRegParam with HasElasticNetParam 
      with HasStandardizationParam with HasFitInterceptParam {
  
}

class DiffAutoRegression(override val uid: String)
  extends Estimator[DiffAutoRegressionModel] with DiffAutoRegressionParams with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("DiffAutoRegression"))

  override def fit(dataset:Dataset[_]):DiffAutoRegressionModel = {

    require($(p) > 0 && $(d) > 0, s"Parameters p, d must be positive")
 
    validateSchema(dataset.schema)
 
    val suning = SuningDiffAutoRegression(
      $(valueCol), $(timeCol), $(p), $(d),
      $(regParam), $(standardization), $(elasticNetParam), $(fitIntercept))

    val model = suning.fit(dataset.toDF)

    val intercept = model.getIntercept
    val weights = model.getWeights

    copyValues(new DiffAutoRegressionModel(uid, intercept, weights).setParent(this))

  }

  override def transformSchema(schema:StructType):StructType = {
    schema
  }

  override def copy(extra:ParamMap):DiffAutoRegression = defaultCopy(extra)
  
}

class DiffAutoRegressionModel(override val uid:String, intercept:Double, weights:Vector)
  extends Model[DiffAutoRegressionModel] with DiffAutoRegressionParams with MLWritable {

  import DiffAutoRegressionModel._

  def getIntercept:Double = intercept
  
  def getWeights:Vector = weights

  def this(intercept:Double, weights:Vector) = {
    this(Identifiable.randomUID("DiffAutoRegressionModel"), intercept, weights)
  }
  
  def evaluate(predictions:Dataset[Row]):String = {
 
    val diffAR = SuningDiffAutoRegression($(valueCol), $(timeCol), $(p), $(d),
      $(regParam), $(standardization), $(elasticNetParam), $(fitIntercept))
 
		val labelCol = diffAR.getLabelCol
		val predictionCol = diffAR.getPredictionCol
				
	  Evaluator.evaluate(predictions, labelCol, predictionCol)
   
  }

  def forecast(dataset:Dataset[_], steps:Int):DataFrame = {
 
    validateSchema(dataset.schema)

    val diffAR = SuningDiffAutoRegression($(valueCol), $(timeCol), $(p), $(d),
      $(regParam), $(standardization), $(elasticNetParam), $(fitIntercept))
    
    val prepared = diffAR.prepareDiffAR(dataset.toDF)
    val featureCols = diffAR.getFeatureCols
    
    val intercept = getIntercept
    val weights = getWeights
    
    val model = LinearRegressionModel.get(intercept,weights)    

    val linearReg = new SuningRegression(featureCols)
    linearReg.setModel(model)
    
    val predictions = linearReg.transform(prepared).orderBy(desc($(timeCol)))  
    diffAR.forecast(predictions, intercept, weights, steps)
    
  }
  
  override def transform(dataset:Dataset[_]):DataFrame = {
 
    validateSchema(dataset.schema)

    val diffAR = SuningDiffAutoRegression($(valueCol), $(timeCol), $(p), $(d),
      $(regParam), $(standardization), $(elasticNetParam), $(fitIntercept))
    
    val prepared = diffAR.prepareDiffAR(dataset.toDF)
    val featureCols = diffAR.getFeatureCols
    
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

  override def copy(extra:ParamMap):DiffAutoRegressionModel = {
    val copied = new DiffAutoRegressionModel(uid, intercept, weights).setParent(parent)
    copyValues(copied, extra)
  }

  override def write: MLWriter = new DiffAutoRegressionModelWriter(this)

}

object DiffAutoRegressionModel extends MLReadable[DiffAutoRegressionModel] {

  private case class Data(intercept: Double, coefficients: Vector)
  
  class DiffAutoRegressionModelWriter(instance: DiffAutoRegressionModel) extends MLWriter {

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

  private class DiffAutoRegressionModelReader extends MLReader[DiffAutoRegressionModel] {

    private val className = classOf[DiffAutoRegressionModel].getName

    override def load(path: String):DiffAutoRegressionModel = {
      
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
      val model = new DiffAutoRegressionModel(metadata.uid, data.intercept, data.coefficients)
      SparkParamsReader.getAndSetParams(model, metadata)
      
      model
      
    }
  }

  override def read: MLReader[DiffAutoRegressionModel] = new DiffAutoRegressionModelReader

  override def load(path: String): DiffAutoRegressionModel = super.load(path)
  
}