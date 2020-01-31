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

import com.suning.spark.regression.LinearRegression
import com.suning.spark.ts.TimeSeriesUtil._
import com.suning.spark.util.{Identifiable, Model, SaveLoad}

import org.apache.spark.SparkContext
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
  
class MovingAverage(override val uid: String, inputCol: String, timeCol: String, q: Int,
                    regParam: Double, standardization: Boolean, elasticNetParam: Double,
                    withIntercept: Boolean, meanOut: Boolean)
  extends TSModel(uid, inputCol, timeCol) {

  def this(inputCol: String, timeCol: String, q: Int,
           regParam: Double, standardization: Boolean = true, elasticNetParam: Double,
           withIntercept: Boolean = false, meanOut: Boolean) =
    this(Identifiable.randomUID("MovingAverage"), inputCol, timeCol, q, regParam, standardization,
      elasticNetParam, withIntercept, meanOut)

  private var lr_ma: LinearRegression = _

  private val residual = "residual"
  private val prediction = "prediction"
  private val label = "label"
  private val lag = "_lag_"
  private val maLabel = "maLabel"

  def getLabelCol:String = maLabel
  
  def getFeatureCols:Array[String] = {

    val features = (1 to q toArray).map(residual + lag + _)
    features
    
  }

  def getPredictionCol:String = "prediction"
  
  override def fitImpl(df: DataFrame): this.type = {
    require(q > 0, s"q can not be 0")
    val arModel = AutoRegression(inputCol, timeCol, q,
      regParam, standardization, elasticNetParam, withIntercept, meanOut)

    arModel.fit(df)

    val residualDF = arModel.transform(df)
      .withColumn(residual, -col(prediction) + col(label))
      .drop(prediction)

    val newDF = TimeSeriesUtil.LagCombinationMA(residualDF, inputCol, residual, timeCol, label, q)
      .filter(col(residual + lag + q).isNotNull)
      .withColumnRenamed(inputCol + lag + "0", maLabel)

    val features = getFeatureCols

    val maxIter = 1000
    val tol = 1E-6

    newDF.persist()

    lr_ma = LinearRegression(features, maLabel, regParam, withIntercept, standardization,
      elasticNetParam, maxIter, tol)

    lr_ma.fit(newDF)

    newDF.unpersist()

    this

  }

  def prepareMA(df:DataFrame): DataFrame = {
    
    require(q > 0, s"q can not be 0")

    val arModel = AutoRegression(inputCol, timeCol, q, regParam, standardization, elasticNetParam,
      withIntercept)

    arModel.fit(df)
    
    val features = "features"
    val residualDF = arModel.transform(df)
      .withColumn(residual, -col(prediction) + col(label))
      .drop(features).drop(prediction)

    val newDF = TimeSeriesUtil.LagCombination(residualDF, residual, timeCol, q, lagsOnly = false)
      .filter(col(residual + lag + q).isNotNull)

    newDF
    
  }
  override def transformImpl(df: DataFrame): DataFrame = {

    val newDF = prepareMA(df)
    lr_ma.transform(newDF)

  }
  
  def forecast(predictions: DataFrame, intercept: Double, weights: Vector, numAhead: Int): DataFrame = {

    val prefix = "residual"
    val lag = "_lag_"

    val listDF = predictions.select(prefix + lag + 0)
      .limit(q).collect().map(_.getDouble(0)).toList

    var listPrediction = listDF
    listPrediction = tsFitDotProduct(listDF, q + 1, q, 0.0, weights, meanValue = 0.0)

    var values = listPrediction.slice(0, q + 1).reverse
    values = values.map(i => i + intercept)

    ((q + 2) to numAhead) foreach { _ =>
      values = values :+ values(q)
    }
    
    forecastResult(predictions, values, numAhead)
    
  }

  override def forecast(df: DataFrame, numAhead: Int): DataFrame = {
    
    require(q > 0, s"q can not be 0")

    val predictions = transform(df).orderBy(desc(timeCol))

    val intercept = getIntercept
    val weights = getWeights
    
    forecast(predictions, intercept, weights, numAhead)
    
  }


  def getIntercept(): Double = {
    lr_ma.getIntercept()
  }

  def getWeights(): Vector = {
    lr_ma.getWeights()
  }

  override def copy(): Model = {
    new MovingAverage(inputCol, timeCol, q,
      regParam, standardization, elasticNetParam, withIntercept, meanOut)
  }

  override def save(path: String): Unit = {
    MovingAverage.save(this, path)
  }

  override def saveHDFS(sc: SparkContext, path: String): Unit = {
    MovingAverage.saveHDFS(sc, this, path)
  }
}

object MovingAverage extends SaveLoad[MovingAverage] {
  def apply(uid: String, inputCol: String,
            timeCol: String, q: Int, regParam: Double, standardization: Boolean,
            elasticNetParam: Double, withIntercept: Boolean, meanOut: Boolean):
  MovingAverage = new MovingAverage(uid, inputCol, timeCol, q, regParam, standardization,
    elasticNetParam, withIntercept, meanOut)

  def apply(inputCol: String, timeCol: String, q: Int, regParam: Double, standardization: Boolean,
            elasticNetParam: Double, withIntercept: Boolean, meanOut: Boolean = false):
  MovingAverage = new MovingAverage(inputCol, timeCol, q, regParam, standardization,
    elasticNetParam, withIntercept, meanOut)
}
