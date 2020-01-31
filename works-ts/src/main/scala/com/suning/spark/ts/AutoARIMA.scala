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

import com.suning.spark.util.{Identifiable, Model, SaveLoad}

import org.apache.spark.SparkContext
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import collection.mutable.Map

class AutoARIMA(override val uid: String, inputCol: String, timeCol: String, p_Max: Int, d_Max: Int, q_Max: Int,
                regParam: Double, standardization: Boolean, elasticNetParam: Double,
                withIntercept: Boolean, meanOut: Boolean, criterion: String)

  extends TSModel(uid, inputCol, timeCol) {
  /*
    For the ARMA(p,q) parameters,
   p: order of the autoregressive part;
   q: order of the moving average part.
   */

  def this(inputCol: String, timeCol: String, p_Max: Int, d_Max: Int, q_Max: Int,
           regParam: Double, standardization: Boolean = true, elasticNetParam: Double,
           withIntercept: Boolean = false, meanOut: Boolean, criterion: String) =
    this(Identifiable.randomUID("AutoARIMA"), inputCol, timeCol, p_Max, d_Max, q_Max, regParam, standardization,
      elasticNetParam, withIntercept, meanOut, criterion)

  private var lr_Autoarima: ARIMA = _
  var criterionValue = Map[(Int, Int, Int), Double]()
  
  private var p_Best: Int = _
  private var d_Best: Int = _
  private var q_Best: Int = _
  
  var best: ((Int, Int, Int), Double) = _

  def getPBest:Int = p_Best

  def getDBest:Int = d_Best

  def getQBest:Int = q_Best
  
  override def fitImpl(df: DataFrame): this.type = {
    val n = df.count().toInt
    criterionValue = criterionCalcul(df, n, criterion)

    println(criterionValue)

    p_Best = (criterionValue.minBy(_._2)._1)._1

    d_Best = (criterionValue.minBy(_._2)._1)._2

    q_Best = (criterionValue.minBy(_._2)._1)._3

    println(s"Best criterion value is ${criterionValue.valuesIterator.min} by p: ${p_Best}. d: ${d_Best}, and q: ${q_Best}")
    //
    df.persist()
    lr_Autoarima = ARIMA(inputCol, timeCol, p_Best, d_Best, q_Best,
      regParam, standardization, elasticNetParam, withIntercept, meanOut)

    lr_Autoarima.fit(df)
    df.unpersist()

    this
  }


  def criterionCalcul(df: DataFrame, n: Int, criterion: String): Map[(Int, Int, Int), Double] = {
    //    does not allow p=q=0

//    (0 to d_Max).map(k => {
//      (1 to q_Max).map(j => {
//        val lr_Autoarima = ARIMA(inputCol, timeCol, 0, k, j,
//          regParam, standardization, elasticNetParam, withIntercept, meanOut)
//        val model = lr_Autoarima.fit(df)
//        val pred = model.transform(df)
//        val residuals = pred.withColumn("residual", -col("prediction") + col("label")).select("residual")
//
//        var criterionIte = TimeSeriesUtil.AIC(residuals, 1, n)
//
//        if (criterion == "aic") {
//          criterionIte = TimeSeriesUtil.AIC(residuals, (k+0+j), n)
//          println(s"AIC value for p: ${0}, d: ${k}, and q: ${j} is ${criterionIte}")
//        } else if (criterion == "bic") {
//          criterionIte = TimeSeriesUtil.BIC(residuals, (k+0+j), n)
//          println(s"BIC value for p: ${0}, d: ${k}, and q: ${j} is ${criterionIte}")
//        } else {
//          criterionIte = TimeSeriesUtil.AICc(residuals, (k+0+j), n)
//          println(s"AICC value for p: ${0}, d: ${k}, and q: ${j} is ${criterionIte}")
//        }
//        criterionValue += (0, k, j) -> criterionIte
//      })
//    })

    /*
     * We restrict to genuine ARIMA models, i.e. p, d, q > 0; note, this
     * is different from the Suning's original implementation
     */
    
    (1 to d_Max).map(k => {
      (1 to p_Max).map(i => {
        (1 to q_Max).map(j => {
          val lr_Autoarima = ARIMA(inputCol, timeCol, i, k, j,
            regParam, standardization, elasticNetParam, withIntercept, meanOut)
          val model = lr_Autoarima.fit(df)
          val pred = model.transform(df)
          val residuals = pred.withColumn("residual", -col("prediction") + col("label")).select("residual")

          var criterionIte = TimeSeriesUtil.AIC(residuals, 1, n)

          if (criterion == "aic") {
            criterionIte = TimeSeriesUtil.AIC(residuals, (k+i+j), n)
            println(s"AIC value for p: ${i}, d: ${k}, and q: ${j} is ${criterionIte}")
          } else if (criterion == "bic") {
            criterionIte = TimeSeriesUtil.BIC(residuals, (k+i+j), n)
            println(s"BIC value for p: ${i}, d: ${k}, and q: ${j} is ${criterionIte}")
          } else {
            criterionIte = TimeSeriesUtil.AICc(residuals, (k+i+j), n)
            println(s"AICC value for p: ${i}, d: ${k}, and q: ${j} is ${criterionIte}")
          }
          criterionValue += (i, k, j) -> criterionIte
        })
      })
    })

    criterionValue

  }

  override def transformImpl(df: DataFrame): DataFrame = {
    lr_Autoarima.transform(df)
  }

  override def forecast(df: DataFrame, numAhead: Int): DataFrame = {
    lr_Autoarima.forecast(df, numAhead)
  }

  def getIntercept(): Double = {
    lr_Autoarima.getIntercept()
  }

  def getWeights(): Vector = {
    lr_Autoarima.getWeights
  }

  override def copy(): Model = {
    new AutoARIMA(inputCol, timeCol, p_Max, d_Max, q_Max, regParam, standardization, elasticNetParam,
      withIntercept, meanOut, criterion)
  }

  override def save(path: String): Unit = {
    AutoARIMA.save(this, path)
  }

  override def saveHDFS(sc: SparkContext, path: String): Unit = {
    AutoARIMA.saveHDFS(sc, this, path)
  }
}

object AutoARIMA extends SaveLoad[AutoARIMA] {
  def apply(uid: String, inputCol: String,
            timeCol: String, p_Max: Int, d_Max: Int, q_Max: Int, regParam: Double, standardization: Boolean,
            elasticNetParam: Double, withIntercept: Boolean, meanOut: Boolean, criterion: String = "aic"):
  AutoARIMA = new AutoARIMA(uid, inputCol, timeCol, p_Max, d_Max, q_Max, regParam, standardization,
    elasticNetParam, withIntercept, meanOut, criterion)

  def apply(inputCol: String, timeCol: String, p_Max: Int, d_Max: Int, q_Max: Int, regParam: Double,
            standardization: Boolean, elasticNetParam: Double, withIntercept: Boolean, meanOut: Boolean, criterion: String):
  AutoARIMA = new AutoARIMA(inputCol, timeCol, p_Max, d_Max, q_Max, regParam, standardization,
    elasticNetParam, withIntercept, meanOut, criterion)
}
