package com.suning.spark.ts
/*
 * Copyright (c) 2016 Suning R&D. All rights reserved.
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
 */

import com.suning.spark.transform.Feature2Vector
import com.suning.spark.ts.TimeSeriesUtil._
import com.suning.spark.util.{Identifiable, Model, SaveLoad}

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.mllib.linalg.{DenseMatrix => OldDenseMatrix,Vector => OldVector, DenseVector => OldDenseVector, Vectors => OldVectors}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
  
class ARYuleWalker(override val uid: String, inputCol: String, timeCol: String, p: Int)
  extends TSModel(uid, inputCol, timeCol) {

  def this(inputCol: String, timeCol: String, p: Int) =
    this(Identifiable.randomUID("ARYuleWalker"), inputCol, timeCol, p)

  var weights: Vector = _

  val features = (1 to p toArray).map(inputCol + "_lag_" + _)
  val label = inputCol + "_lag_0"

  val f2v = Feature2Vector(features, label)

  val predict = udf((v: Vector) => {
    var res = 0.0
    val r = v.size
    (0 until r).foreach(
      i => {
        res += v(i) * weights(i)
      }
    )
    res
  })

  def getLabelCol:String = label

  def getFeatureCols:Array[String] = features
  
  def getPredictionCol:String = "prediction"
  
  override def fitImpl(df: DataFrame): this.type = {
    require(p > 0, s"p can not be 0")

    val newDF = df.select(inputCol, timeCol)
    val corrs = TimeSeriesUtil.AutoCorrelationFunc(df, inputCol, timeCol, p, twoDecimal = false)

    val corrslist = corrs.reverse ++ corrs.tail

    val corrslist0 = OldVectors.dense(corrs.slice(from = 1, until = p + 1))

    val denseData = (0 to p - 1 ).toSeq.map(i => {
      org.apache.spark.mllib.linalg.Vectors.dense(corrslist.slice(from = p - i, until = 2 * p - i))
    })

    val denseMat: RowMatrix = new RowMatrix(df.sqlContext.sparkContext.parallelize(denseData, 2))
    val InversedenseMat = TimeSeriesUtil.computeInverse(denseMat)

    weights = Vectors.dense(InversedenseMat.multiply(corrslist0).values)
    this
  }
  
  def prepareARYuleWalker(df:DataFrame): DataFrame = {
    
    require(p > 0, s"p can not be 0")
    var newDF = df.select(inputCol, timeCol)

    newDF = f2v.transform(TimeSeriesUtil.LagCombination(newDF, inputCol, timeCol, p)
      .filter(col(inputCol + "_lag_" + p).isNotNull))
   
    newDF
    
  }
  override def transformImpl(df: DataFrame): DataFrame = {
    
    val newDF = prepareARYuleWalker(df)
    newDF.withColumn("prediction", predict(newDF("features")))

  }

  def forecast(predictions: DataFrame, meanValue:Double, weights: Vector, numAhead: Int): DataFrame = {

    val prefix = ""
    val lag = "_lag_"
    
    var listDF = predictions.select(inputCol + prefix + lag + 0)
      .limit(p).collect().map(_.getDouble(0)).toList

    listDF = listDF.map(i => i - meanValue)

    val listPrediction = tsFitDotProduct(listDF, numAhead, p, intercept = 0.0, weights, meanValue = meanValue)

    val values = listPrediction.slice(0, numAhead).reverse
    forecastResult(predictions, values, numAhead)

  }

  override def forecast(df: DataFrame, numAhead: Int): DataFrame = {

    require(p > 0, s"p can not be 0")

    val predictions = transform(df).orderBy(desc(timeCol))
    
    val meanValue = getDouble(df.select(mean(inputCol)).collect()(0).get(0))
    val weights = getCoefficients
    
    forecast(predictions, meanValue, weights, numAhead)

  }

  def setCoefficients(weights:Vector) {
    this.weights = weights
  }

  def getCoefficients(): Vector = {
    weights
  }

  override def copy(): Model = {
    new ARYuleWalker(inputCol, timeCol, p)
  }

  override def save(path: String): Unit = {
    ARYuleWalker.save(this, path)
  }

  override def saveHDFS(sc: SparkContext, path: String): Unit = {
    ARYuleWalker.saveHDFS(sc, this, path)
  }
}

object ARYuleWalker extends SaveLoad[ARYuleWalker] {
  def apply(uid: String, inputCol: String, timeCol: String, p: Int):
  ARYuleWalker = new ARYuleWalker(uid, inputCol, timeCol, p)

  def apply(inputCol: String, timeCol: String, p: Int):
  ARYuleWalker = new ARYuleWalker(inputCol, timeCol, p)
}
