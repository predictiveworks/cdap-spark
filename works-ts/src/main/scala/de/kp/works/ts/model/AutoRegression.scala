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
import com.suning.spark.ts.{AutoRegression => SuningAutoRegression}
import com.suning.spark.regression.{LinearRegression => SuningRegression}
import de.kp.works.core.recording.RegressorEvaluator
import org.apache.hadoop.fs.Path
import org.apache.spark.ml._
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

trait AutoRegressionParams extends ModelParams with HasPParam 
      with HasRegParam with HasElasticNetParam
      with HasStandardizationParam with HasFitInterceptParam
      with HasMeanOutParam {
  
}

class AutoRegression(override val uid: String)
  extends Estimator[AutoRegressionModel] with AutoRegressionParams with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("AutoRegression"))

  override def fit(dataset:Dataset[_]):AutoRegressionModel = {

    require($(p) > 0, s"Parameter p  must be positive")
 
    validateSchema(dataset.schema)
 
    val suning = SuningAutoRegression(
      $(valueCol), $(timeCol), $(p),
      $(regParam), $(standardization), $(elasticNetParam), $(fitIntercept), $(meanOut))
    
    val model = suning.fit(dataset.toDF)

    val intercept = model.getIntercept
    val weights = model.getWeights

    copyValues(new AutoRegressionModel(uid, intercept, weights).setParent(this))

  }

  override def transformSchema(schema:StructType):StructType = {
    schema
  }

  override def copy(extra:ParamMap):AutoRegression = defaultCopy(extra)
  
}

class AutoRegressionModel(override val uid:String, intercept:Double, weights:Vector)
  extends Model[AutoRegressionModel] with AutoRegressionParams with MLWritable {

  import AutoRegressionModel._

  def this(intercept:Double, weights:Vector) = {
    this(Identifiable.randomUID("AutoRegressionModel"), intercept, weights)
  }

  def getIntercept:Double = intercept
  
  def getWeights:Vector = weights

  def evaluate(predictions:Dataset[Row]):String = {

    val ar = SuningAutoRegression($(valueCol), $(timeCol), $(p),
      $(regParam), $(standardization), $(elasticNetParam), $(fitIntercept), $(meanOut))
 
		val labelCol = ar.getLabelCol
		val predictionCol = ar.getPredictionCol
				
	  RegressorEvaluator.evaluate(predictions, labelCol, predictionCol)
    
  }

  def forecast(dataset:Dataset[Row], steps:Int):DataFrame = {
 
    validateSchema(dataset.schema)

    val ar = SuningAutoRegression($(valueCol), $(timeCol), $(p),
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

    val ar = SuningAutoRegression($(valueCol), $(timeCol), $(p),
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

  override def copy(extra:ParamMap):AutoRegressionModel = {
    val copied = new AutoRegressionModel(uid, intercept, weights).setParent(parent)
    copyValues(copied, extra)
  }

  override def write: MLWriter = new AutoRegressionModelWriter(this)

}

object AutoRegressionModel extends MLReadable[AutoRegressionModel] {

  private case class Data(intercept: Double, coefficients: Vector)
  
  class AutoRegressionModelWriter(instance: AutoRegressionModel) extends MLWriter {

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

  private class AutoRegressionModelReader extends MLReader[AutoRegressionModel] {

    private val className = classOf[AutoRegressionModel].getName

    override def load(path: String):AutoRegressionModel = {
      
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
      val model = new AutoRegressionModel(metadata.uid, data.intercept, data.coefficients)
      SparkParamsReader.getAndSetParams(model, metadata)
      
      model
      
    }
  }

  override def read: MLReader[AutoRegressionModel] = new AutoRegressionModelReader

  override def load(path: String): AutoRegressionModel = super.load(path)
  
}