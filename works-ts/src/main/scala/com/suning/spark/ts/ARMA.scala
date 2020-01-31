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
import com.suning.spark.util.{Identifiable, Model, SaveLoad}

import org.apache.spark.SparkContext
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class ARMA(override val uid: String, inputCol: String, timeCol: String, p: Int, q: Int,
           regParam: Double, standardization: Boolean, elasticNetParam: Double,
           withIntercept: Boolean)
  extends TSModel(uid, inputCol, timeCol) {
  /*
    For the ARMA(p,q) parameters,
   p: order of the autoregressive part;
   q: order of the moving average part.
   */

  def this(inputCol: String, timeCol: String, p: Int, q: Int, regParam: Double,
           standardization: Boolean = true,
           elasticNetParam: Double, withIntercept: Boolean = false) =
    this(Identifiable.randomUID("ARMA"), inputCol, timeCol, p, q, regParam, standardization,
      elasticNetParam, withIntercept)

  private var lr_arma: LinearRegression = _
  private var arModel: AutoRegression = _
  private var maModel: MovingAverage = _

  private val lag = "_lag_"
  private val label = "label"
  private val residual = "residual"

  def getLabelCol:String = inputCol + lag + 0
  
  def getFeatureCols:Array[String] = {

    val features_ar = (1 to p).map(inputCol + lag + _).toArray
    val features_ma = (1 to q).map(residual + lag + _).toArray

    features_ar ++ features_ma
    
  }

  def getPredictionCol:String = "prediction"
  
  def fitARMA(df: DataFrame): Unit = {

    val prediction = "prediction"
    val feature = "features"
    val maxPQ = math.max(p, q)

    val newDF = TimeSeriesUtil.LagCombination(df, inputCol, timeCol, maxPQ).
      filter(col(inputCol + lag + maxPQ).isNotNull)

    //For MA model.
    val lr_ar = AutoRegression(inputCol, timeCol, maxPQ, regParam, standardization, elasticNetParam, false, false)

    val model_ma = lr_ar.fit(newDF)
    val pred_ma = model_ma.transform(newDF)

    // get the residuals as (-truth + predicted)
    val residualDF = pred_ma.withColumn(residual, -col(prediction) + col(label))

    val newDF_arma = TimeSeriesUtil.LagCombinationARMA(residualDF, inputCol, residual, timeCol,
      prediction, p = p, q = q)
      .filter(col(residual + lag + q).isNotNull)
      .drop(prediction)
      .drop(feature)

    val features = getFeatureCols
    val armaLabel = getLabelCol

    val maxIter = 1000
    val tol = 1E-6

    newDF_arma.persist()

    lr_arma = LinearRegression(features, armaLabel, regParam, withIntercept, standardization,
      elasticNetParam, maxIter, tol)

    lr_arma.fit(newDF_arma)

    newDF_arma.unpersist()
  }

  override def fitImpl(df: DataFrame): this.type = {
    require(p > 0 || q > 0, s"p or q can not be 0 at the same time")
    if (p == 0 || q == 0) {
      if (q == 0) {
        arModel = AutoRegression(inputCol, timeCol, p, regParam, standardization, elasticNetParam,
          withIntercept)
        arModel.fit(df)
      }
      else {
        maModel = MovingAverage(inputCol, timeCol, q, regParam, standardization, elasticNetParam,
          withIntercept)
        maModel.fit(df)
      }
    }
    else {
      fitARMA(df)
    }
    this
  }
  /*
   * __KUP__ We externalize the prepration steps
   * to enable model prediction from reloaded model
   */  
  def prepareARMA(df: DataFrame): DataFrame = {
    
    val lag = "_lag_"
    val residual = "residual"
    val label = "label"
    val prediction = "prediction"
    val feature = "features"

    val maxPQ = math.max(p, q)
    val newDF = TimeSeriesUtil.LagCombination(df, inputCol, timeCol, maxPQ).
      filter(col(inputCol + lag + maxPQ).isNotNull)
    
    //For MA model.
    val lr_ar = AutoRegression(inputCol, timeCol, maxPQ, regParam, standardization, elasticNetParam, withIntercept = false, meanOut = false)

    val model_ma = lr_ar.fit(newDF)
    val pred_ma = model_ma.transform(newDF)
    
    // get the residuals as (-truth + predicted)
    val residualDF = pred_ma.withColumn(residual, -col(prediction) + col(label))
    val newDF_arma = TimeSeriesUtil.LagCombinationARMA(residualDF, inputCol, residual, timeCol,
      prediction, p = p, q = q)
      .filter(col(residual + lag + q).isNotNull)
      .drop(prediction)
    
    newDF_arma
    
  }
  
  def transformARMA(df: DataFrame): DataFrame = {
    
    val newDF_arma = prepareARMA(df)
    lr_arma.transform(newDF_arma)

  }
  
  def forecast(predictions: DataFrame, intercept:Double, weights:Vector, numAhead: Int): DataFrame = {

    val values = if (p == 0 || q == 0) {
      if (q == 0) {        
        TimeSeriesUtil.tsForecastAR(predictions, numAhead, inputCol, timeCol, p,
          intercept, weights)

      } else {
        TimeSeriesUtil.tsForecastMA(predictions, numAhead, inputCol, timeCol, q,
          intercept, weights)
          
      }
      
    } else {
      TimeSeriesUtil.tsForecastARMAModel(predictions, numAhead, inputCol, timeCol, p, q,
        intercept, weights)

    }
    
    forecastResult(predictions, values, numAhead)
    
  }

  override def forecast(df: DataFrame, numAhead: Int): DataFrame = {

    require(p > 0 || q > 0, s"p or q can not be 0 at the same time")

    val predictions = transform(df)
    
    val intercept = getIntercept()
    val weights = getWeights()

    forecast(predictions, intercept, weights, numAhead)
    
  }

  override def transformImpl(df: DataFrame): DataFrame = {
    require(p > 0 || q > 0, s"p or q can not be 0 at the same time")
    if (p == 0 || q == 0) {
      if (q == 0) {
        arModel.transform(df)
      }
      else {
        maModel.transform(df)
      }
    }
    else {
      transformARMA(df)
    }
  }


  def getIntercept(): Double = {
    if (p == 0 || q == 0) {
      if (q == 0) {
        arModel.getIntercept()
      }
      else {
        maModel.getIntercept()
      }
    }
    else {
      lr_arma.getIntercept()
    }
  }

  def getWeights(): Vector = {
    if (p == 0 || q == 0) {
      if (q == 0) {
        arModel.getWeights()
      }
      else {
        maModel.getWeights()
      }
    }
    else {
      lr_arma.getWeights()
    }
  }

  override def copy(): Model = {
    new ARMA(inputCol, timeCol, p, q, regParam, standardization, elasticNetParam, withIntercept)
  }

  override def save(path: String): Unit = {
    ARMA.save(this, path)
  }

  override def saveHDFS(sc: SparkContext, path: String): Unit = {
    ARMA.saveHDFS(sc, this, path)
  }
}

object ARMA extends SaveLoad[ARMA] {
  def apply(uid: String, inputCol: String,
            timeCol: String, p: Int, q: Int, regParam: Double, standardization: Boolean,
            elasticNetParam: Double, withIntercept: Boolean):
  ARMA =
    new ARMA(uid, inputCol, timeCol, p, q, regParam, standardization, elasticNetParam,
      withIntercept)

  def apply(inputCol: String, timeCol: String, p: Int, q: Int, regParam: Double,
            standardization: Boolean, elasticNetParam: Double, withIntercept: Boolean):
  ARMA = new ARMA(inputCol, timeCol, p, q, regParam, standardization, elasticNetParam, withIntercept)

}
