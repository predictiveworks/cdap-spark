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

import com.suning.spark.ts.{ARYuleWalker => SuningARYuleWalker}

import org.apache.hadoop.fs.Path

import org.apache.spark.ml._
import org.apache.spark.ml.linalg.Vector

import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._

import org.apache.spark.ml.util._

import org.apache.spark.sql._
import org.apache.spark.sql.types._

trait ARYuleWalkerParams extends ModelParams with HasPParam {
  
}

class ARYuleWalker(override val uid: String)
  extends Estimator[ARYuleWalkerModel] with ARYuleWalkerParams with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("ARYuleWalker"))

  override def fit(dataset:Dataset[_]):ARYuleWalkerModel = {

    require($(p) > 0 , s"Parameter p must be positive")
 
    val suning = SuningARYuleWalker(
      $(valueCol), $(timeCol), $(p))
      
    val model = suning.fit(dataset.toDF)

    val weights = model.getCoefficients

    copyValues(new ARYuleWalkerModel(uid, weights).setParent(this))
    
  }

  override def transformSchema(schema:StructType):StructType = {
    schema
  }

  override def copy(extra:ParamMap):ARYuleWalker = defaultCopy(extra)
  
}

class ARYuleWalkerModel(override val uid:String, weights:Vector)
  extends Model[ARYuleWalkerModel] with ARYuleWalkerParams with MLWritable {

  import ARYuleWalkerModel._

  def this(weights:Vector) = {
    this(Identifiable.randomUID("ARYuleWalkerModel"), weights)
  }
  
  def getWeights:Vector = weights

  def evaluate(predictions:Dataset[Row]):String = {

    val yuleWalker = SuningARYuleWalker($(valueCol), $(timeCol), $(p))
 
		val labelCol = yuleWalker.getLabelCol
		val predictionCol = yuleWalker.getPredictionCol
				
	  Evaluator.evaluate(predictions, labelCol, predictionCol)
    
  }
  
  override def transform(dataset:Dataset[_]):DataFrame = {

    val yuleWalker = SuningARYuleWalker($(valueCol), $(timeCol), $(p))

    val weights = getWeights
    yuleWalker.setCoefficients(weights)
    
    yuleWalker.transform(dataset.toDF)

  }

  override def transformSchema(schema:StructType):StructType = {
    schema
  }

  override def copy(extra:ParamMap):ARYuleWalkerModel = {
    val copied = new ARYuleWalkerModel(uid, weights).setParent(parent)
    copyValues(copied, extra)
  }

  override def write: MLWriter = new ARYuleWalkerModelWriter(this)

}

object ARYuleWalkerModel extends MLReadable[ARYuleWalkerModel] {

  private case class Data(weights: Vector)
   
  class ARYuleWalkerModelWriter(instance: ARYuleWalkerModel) extends MLWriter {

    override def save(path:String): Unit = {
      super.save(path)
    }
    
    override def saveImpl(path: String): Unit = {
      
      /* Save metadata & params */
      SparkParamsWriter.saveMetadata(instance, path, sc)
      
       val coefficients = instance.getWeights
      
      val data = Data(coefficients)
      val dataPath = new Path(path, "data").toString
      
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)      
      
    }
    
  }

  private class ARYuleWalkerModelReader extends MLReader[ARYuleWalkerModel] {

    private val className = classOf[ARYuleWalkerModel].getName

    override def load(path: String):ARYuleWalkerModel = {
      
      /* Read metadata & params */
      val metadata = SparkParamsReader.loadMetadata(path, sc, className)
      /*
       * Retrieve weights
       */
      val sparkSession = super.sparkSession
      import sparkSession.implicits._

      val dataPath = new Path(path, "data").toString
      val data: Data =  sparkSession.read.parquet(dataPath).as[Data].collect.head
      /*
       * Reconstruct trained model instance
       */
      val model = new ARYuleWalkerModel(metadata.uid, data.weights)
      SparkParamsReader.getAndSetParams(model, metadata)
      
      model
      
    }
  }

  override def read: MLReader[ARYuleWalkerModel] = new ARYuleWalkerModelReader

  override def load(path: String): ARYuleWalkerModel = super.load(path)
  
}