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

import com.suning.spark.ts.{ARMA => SuningARMA}
import com.suning.spark.regression.{LinearRegression => SuningRegression}
import de.kp.works.core.ml.RegressorEvaluator
import org.apache.hadoop.fs.Path
import org.apache.spark.ml._
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

trait ARMAParams extends ModelParams 
      with HasPParam with HasQParam
      with HasRegParam with HasElasticNetParam 
      with HasStandardizationParam with HasFitInterceptParam {
  
  setDefault(standardization -> true, fitIntercept -> false)
  
}

class ARMA(override val uid: String)
  extends Estimator[ARMAModel] with ARMAParams with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("ARMA"))

  override def fit(dataset:Dataset[_]):ARMAModel = {

    require($(p) > 0 && $(q) > 0, s"Parameters p, q must be positive")
 
    validateSchema(dataset.schema)
 
    val suning = SuningARMA(
      $(valueCol), $(timeCol), $(p), $(q),
      $(regParam), $(standardization), $(elasticNetParam), $(fitIntercept))
      
    val model = suning.fit(dataset.toDF)

    val intercept = model.getIntercept
    val weights = model.getWeights

    copyValues(new ARMAModel(uid, intercept, weights).setParent(this))
    
  }

  override def transformSchema(schema:StructType):StructType = {
    schema
  }

  override def copy(extra:ParamMap):ARMA = defaultCopy(extra)
  
}

class ARMAModel(override val uid:String, intercept:Double, weights:Vector)
  extends Model[ARMAModel] with ARMAParams with MLWritable {

  import ARMAModel._

  def this(intercept:Double,weights:Vector) = {
    this(Identifiable.randomUID("ARMAModel"),intercept,weights)
  }

  def getIntercept:Double = intercept
  
  def getWeights:Vector = weights
   
  def evaluate(predictions:Dataset[Row]):String = {

    val arma = SuningARMA($(valueCol), $(timeCol), $(p), $(q),
      $(regParam), $(standardization), $(elasticNetParam), $(fitIntercept))
 
		val labelCol = arma.getLabelCol
		val predictionCol = arma.getPredictionCol
				
	  RegressorEvaluator.evaluate(predictions, labelCol, predictionCol)
    
  }
 
  def forecast(dataset:Dataset[_], steps:Int):DataFrame = {
 
    validateSchema(dataset.schema)

    val arma = SuningARMA($(valueCol), $(timeCol), $(p), $(q),
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

    val arma = SuningARMA($(valueCol), $(timeCol), $(p), $(q),
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

  override def copy(extra:ParamMap):ARMAModel = {
    val copied = new ARMAModel(uid,intercept,weights).setParent(parent)
    copyValues(copied, extra)
  }

  override def write: MLWriter = new ARMAModelWriter(this)

}

object ARMAModel extends MLReadable[ARMAModel] {

  private case class Data(intercept: Double, coefficients: Vector)
  
  class ARMAModelWriter(instance: ARMAModel) extends MLWriter {

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

  private class ARMAModelReader extends MLReader[ARMAModel] {

    private val className = classOf[ARMAModel].getName

    override def load(path: String):ARMAModel = {
      
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
      val model = new ARMAModel(metadata.uid, data.intercept, data.coefficients)
      SparkParamsReader.getAndSetParams(model, metadata)
      
      model
      
    }
  }

  override def read: MLReader[ARMAModel] = new ARMAModelReader

  override def load(path: String): ARMAModel = super.load(path)
  
}