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
  
class AutoRegression(override val uid: String, inputCol: String, timeCol: String, p: Int,
                     regParam: Double, standardization: Boolean, elasticNetParam: Double,
                     withIntercept: Boolean, meanOut: Boolean)
  extends TSModel(uid, inputCol, timeCol) {

  def this(inputCol: String, timeCol: String, p: Int,
           regParam: Double, standardization: Boolean, elasticNetParam: Double,
           withIntercept: Boolean, meanOut: Boolean) =
    this(Identifiable.randomUID("AutoRegression"), inputCol, timeCol, p, regParam, standardization,
      elasticNetParam, withIntercept, meanOut)

  def this(inputCol: String, timeCol: String, p: Int, regParam: Double, standardization: Boolean,
           elasticNetParam: Double, withIntercept: Boolean) =
    this(Identifiable.randomUID("AutoRegression"), inputCol, timeCol, p, regParam, standardization,
      elasticNetParam, withIntercept, false)


  private var lr_ar: LinearRegression = _

  override def fitImpl(df: DataFrame): this.type = {

    require(p > 0, s"p can not be 0")

    val prefix = if (meanOut) "_meanOut" else ""
    val lag = "_lag_"
    val label = inputCol + prefix + lag + (0)
    val r = 1 to p
    val features = r.map(inputCol + prefix + lag + _).toArray

    val newDF = TimeSeriesUtil.LagCombination(df, inputCol, timeCol, p, lagsOnly = false, meanOut)
      .filter(col(inputCol + prefix + lag + p).isNotNull)

    val maxIter = 1000
    val tol = 1E-6

    newDF.persist()

    lr_ar = LinearRegression(features, label, regParam, withIntercept, standardization,
      elasticNetParam, maxIter, tol)

    lr_ar.fit(newDF)
    newDF.unpersist()

    this
  }

  def getFeatureCols:Array[String] = {

    val prefix = if (meanOut) "_meanOut" else ""
    val lag = "_lag_"

    val features = (1 to p).map(inputCol + prefix + lag + _).toArray
    features
    
  }
  
  def prepareAR(df:DataFrame):DataFrame = {
    
    require(p > 0, s"p can not be 0")
    
    val prefix = if (meanOut) "_meanOut" else ""
    val lag = "_lag_"
    
    val newDF = TimeSeriesUtil.LagCombination(df, inputCol, timeCol, p, lagsOnly = false, meanOut)
      .filter(col(inputCol + prefix + lag + p).isNotNull)
    
      newDF
      
  }
  
  override def transformImpl(df: DataFrame): DataFrame = {

    val newDF = prepareAR(df)
    lr_ar.transform(newDF)

  }

  override def forecast(df: DataFrame, numAhead: Int): List[Double] = {

    require(p > 0, s"p can not be 0")

    if (lr_ar == null) fit(df)

    val newDF = transform(df)

    val prefix = if (meanOut) "_meanOut" else ""
    val lag = "_lag_"
    var listPrediction = newDF.orderBy(desc(timeCol)).select(inputCol + prefix + lag + 0)
      .limit(p).collect().map(_.getDouble(0)).toList

    val meanValue = getDouble(df.select(mean(inputCol)).collect()(0).get(0))

    if (meanOut) {
      listPrediction = tsFitDotProduct(listPrediction, numAhead, p, getIntercept(), getWeights(),
        meanValue = meanValue)
    } else {
      listPrediction = tsFitDotProduct(listPrediction, numAhead, p, getIntercept(), getWeights(),
        meanValue = 0.0)
    }

    val prediction = listPrediction.slice(0, numAhead).reverse
    prediction
  }

  def getIntercept(): Double = {
    lr_ar.getIntercept()
  }

  def getWeights(): Vector = {
    lr_ar.getWeights()
  }

  override def copy(): Model = {
    new AutoRegression(inputCol, timeCol, p, regParam, standardization, elasticNetParam,
      withIntercept, meanOut)
  }

  override def save(path: String): Unit = {
    AutoRegression.save(this, path)
  }

  override def saveHDFS(sc: SparkContext, path: String): Unit = {
    AutoRegression.saveHDFS(sc, this, path)
  }
}

object AutoRegression extends SaveLoad[AutoRegression] {
  def apply(uid: String, inputCol: String,
            timeCol: String, p: Int, regParam: Double, standardization: Boolean,
            elasticNetParam: Double, withIntercept: Boolean, meanOut: Boolean = false):
  AutoRegression = new AutoRegression(uid, inputCol, timeCol, p, regParam, standardization,
    elasticNetParam, withIntercept, meanOut)

  def apply(inputCol: String, timeCol: String, p: Int, regParam: Double, standardization: Boolean,
            elasticNetParam: Double, withIntercept: Boolean, meanOut: Boolean):
  AutoRegression = new AutoRegression(inputCol, timeCol, p, regParam, standardization,
    elasticNetParam, withIntercept, meanOut)

  def apply(inputCol: String, timeCol: String, p: Int, regParam: Double, standardization: Boolean,
            elasticNetParam: Double, withIntercept: Boolean):
  AutoRegression = new AutoRegression(inputCol, timeCol, p, regParam, standardization,
    elasticNetParam, withIntercept, false)

}
