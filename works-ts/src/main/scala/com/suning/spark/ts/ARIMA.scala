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
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/** For the ARIMA(p,d,q) parameters,
  * p: order of the autoregressive part;
  * d: degree of first differencing involved;
  * q: order of the moving average part.
  */

class ARIMA(override val uid: String, inputCol: String, timeCol: String, p: Int, d: Int, q: Int,
            regParam: Double, standardization: Boolean, elasticNetParam: Double,
            withIntercept: Boolean, meanOut: Boolean)
  extends TSModel(uid, inputCol, timeCol) {


  def this(inputCol: String, timeCol: String, p: Int, d: Int, q: Int,
           regParam: Double, standardization: Boolean = true,
           elasticNetParam: Double, withIntercept: Boolean = false, meanOut: Boolean) =
    this(Identifiable.randomUID("ARIMA"), inputCol, timeCol, p, d, q, regParam, standardization,
      elasticNetParam, withIntercept, meanOut)

  private var lr_arima: LinearRegression = _
  private var armaModel: ARMA = _
  private var darModel: DiffAutoRegression = _

  private val lag = "_lag_"
  private val label = "label"
  private val prediction = "prediction"
  private val residual = "residual"
  private val feature = "features"

  /*
   * Columns restricted to ARIMA processing with 
   * positive model parameters
   */
  def getLabelCol:String = inputCol + "_diff_" + d + "_lag_" + 0
  
  def getFeatureCols:Array[String] = {

    val lag = "_lag_"
    val residual = "residual"

    val diff = "_diff_" + d

    val features_ar = (1 to p).map(inputCol + diff + lag + _).toArray
    val features_ma = (1 to q).map(residual + lag + _).toArray
 
    features_ar ++ features_ma
    
  }
  
  def getPredictionCol:String = "prediction"
  
  def fitARIMA(df: DataFrame): Unit = {

    // calculate residual
    val maxPQ = math.max(p, q)

    /*
     * STEP #1: Train autoregression model for MA and predict moving average
     */
    val arModel = AutoRegression(inputCol, timeCol, maxPQ,
      regParam, standardization, elasticNetParam, withIntercept, meanOut)

    arModel.fit(df)
    val pred_ma = arModel.transform(df)
    /*
     * STEP #2: Retrieve the residuals as (-truth + predicted)
     */
    val residualDF = pred_ma.withColumn(residual, -col(prediction) + col(label))

    var newDF_dee = TimeSeriesUtil.DiffCombination(residualDF, inputCol, timeCol, p, d, lagsOnly = false)

    val newDF_arima = TimeSeriesUtil.LagCombinationARMA(newDF_dee,
      inputCol, residual, timeCol, prediction, p = p, q = q, d = d, arimaFeature = true)
      .filter(col(residual + lag + q).isNotNull)
      .drop(prediction)
      .drop(feature)

    val features = getFeatureCols
    val arimaLabel = getLabelCol

    val maxIter = 1000
    val tol = 1E-6

    newDF_arima.persist()

    lr_arima = LinearRegression(features, arimaLabel, regParam, withIntercept, standardization, elasticNetParam, maxIter, tol)
    lr_arima.fit(newDF_arima)
    
    newDF_arima.unpersist()

  }
  /*
   * This method combines 3 different models with a 
   * single method 'fitImpl'
   */
  override def fitImpl(df: DataFrame): this.type = {
    //    require(p > q, "For ARIMA(p,d,q), p should be large than q.")
    //Here is to introduce differencing, d for diff.
    require(p > 0 || q > 0, s"p or q can not be 0 at the same time")
    if (d == 0) {

      armaModel =
        ARMA(inputCol, timeCol, p, q, regParam, standardization, elasticNetParam, withIntercept)
      armaModel.fit(df)
    }
    else {
      if (q == 0) {
        darModel = DiffAutoRegression(inputCol, timeCol, p, d, regParam, standardization,
          elasticNetParam, withIntercept)
        darModel.fit(df)
      }
      else {
        /*
         * This ARIMA channel finally trains a LinearRegression model
         */
        fitARIMA(df)
      }
    }
    this
  }
  /*
   * __KUP__ We externalize the prepration steps
   * to enable model prediction from reloaded model
   */
  def prepareARIMA(df: DataFrame): DataFrame = {
    
    val prefix = if (meanOut) "_meanOut" else ""
    val lag = "_lag_"
    val label = "label"
    val feature = "features"
    val prediction = "prediction"
    val residual = "residual"

    val maxPQ = math.max(p, q)

    val newDF = TimeSeriesUtil.LagCombination(df, inputCol,
      timeCol, maxPQ, lagsOnly = false, meanOut).filter(col(inputCol + prefix + lag + maxPQ).isNotNull)

    /*
     * STEP #1: Moving Average  
     */
    val lr_ar = AutoRegression(inputCol, timeCol, maxPQ,
      regParam, standardization, elasticNetParam, withIntercept, meanOut = false)

    lr_ar.fit(newDF)
    val pred_ma = lr_ar.transform(newDF)
    /*
     * STEP #2: Get the residuals as (-truth + predicted)
     */
    val residualDF = pred_ma.withColumn(residual, -col(prediction) + col(label))

    var newDF_dee = TimeSeriesUtil.DiffCombination(residualDF, inputCol, timeCol, p, d, false)

    val newDF_arima = TimeSeriesUtil.LagCombination(newDF_dee,
      residual, timeCol, q, lagsOnly = false, meanOut = false)
      .filter(col(residual + lag + q).isNotNull)
      .drop(prediction)
      .drop(label)
      .drop(feature)

    newDF_arima

  }

  def transformARIMA(df: DataFrame): DataFrame = {

    val newDF_arima = prepareARIMA(df)
    lr_arima.transform(newDF_arima)

  }
  
  override def transformImpl(df: DataFrame): DataFrame = {

    require(p > 0 || q > 0, s"p or q can not be 0 at the same time")
    if (d == 0) {
      armaModel.transform(df)
    }
    else {
      if (q == 0) {
        darModel.transform(df)
      }
      else {
        transformARIMA(df)
      }
    }
  }

  def forecastARIMA(df: DataFrame, numAhead: Int): List[Double] = {
    if (lr_arima == null) {
      fitARIMA(df)
    }
    val newDF = transformARIMA(df).orderBy(desc(timeCol))

    val diff = "_diff_" + d
    val residual = "residual"
    val lag = "_lag_"
    var listDiff = newDF.select(inputCol + diff + lag + 0)
      .limit(p).collect().map(_.getDouble(0)).toList
    var listResi = newDF.select(residual + lag + 0)
      .limit(q).collect().map(_.getDouble(0)).toList

    var listPrev = List[Double](
      getDouble(newDF.select(inputCol).limit(1).collect()(0).get(0))
    )

    val weights = getWeights()
    val intercept = getIntercept()

    val weightsDAR = Vectors.dense(weights.toArray.slice(0, p))
    val weightsMA = Vectors.dense(weights.toArray.slice(p, p + q))

    (0 until numAhead).foreach {
      j => {
        val vecDAR = Vectors.dense(listDiff.slice(0, p).toArray)
        val vecMA = Vectors.dense(listResi.slice(0, q).toArray)
        var diff = 0.0
        var resi = 0.0
        (0 until p).foreach(
          i => {
            diff += vecDAR(i) * weightsDAR(i)
          }
        )
        (0 until q).foreach(
          i => {
            resi += vecMA(i) * weightsMA(i)
          }
        )
        diff = diff + resi + intercept

        listDiff = diff :: listDiff
        listResi = resi :: listResi
        listPrev = (diff + listPrev(0)) :: listPrev
      }
    }
    listPrev.reverse.tail
  }

  override def forecast(df: DataFrame, numAhead: Int): List[Double] = {
    require(p > 0 || q > 0, s"p or q can not be 0 at the same time")

    if (armaModel == null || darModel == null || lr_arima == null) {
      fit(df)
    }

    if (d == 0) {
      armaModel.forecast(df, numAhead)
    }
    else {
      if (q == 0) {
        darModel.forecast(df, numAhead)
      }
      else {
        forecastARIMA(df, numAhead)
      }
    }
  }


  def getIntercept(): Double = {
    if (d == 0) {
      armaModel.getIntercept()
    }
    else {
      if (q == 0) {
        darModel.getIntercept()
      }
      else {
        lr_arima.getIntercept()
      }
    }
  }

  def getWeights(): Vector = {
    if (d == 0) {
      armaModel.getWeights()
    }
    else {
      if (q == 0) {
        darModel.getWeights()
      }
      else {
        lr_arima.getWeights()
      }
    }
  }

  override def copy(): Model = {
    new ARIMA(inputCol, timeCol, p, d, q, regParam, standardization, elasticNetParam, withIntercept, meanOut)
  }

  override def save(path: String): Unit = {
    ARIMA.save(this, path)
  }

  override def saveHDFS(sc: SparkContext, path: String): Unit = {
    ARIMA.saveHDFS(sc, this, path)
  }
}

object ARIMA extends SaveLoad[ARIMA] {
  def apply(uid: String, inputCol: String, timeCol: String, p: Int, d: Int, q: Int,
            regParam: Double, standardization: Boolean,
            elasticNetParam: Double, withIntercept: Boolean, meanOut: Boolean):
  ARIMA =
    new ARIMA(uid, inputCol, timeCol, p, d, q, regParam, standardization, elasticNetParam,
      withIntercept, meanOut)

  def apply(inputCol: String, timeCol: String, p: Int, d: Int, q: Int,
            regParam: Double, standardization: Boolean,
            elasticNetParam: Double, withIntercept: Boolean, meanOut: Boolean = true):
  ARIMA =
    new ARIMA(inputCol, timeCol, p, d, q, regParam, standardization, elasticNetParam, withIntercept, meanOut)

}
