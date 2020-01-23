package de.kp.works.text.sentiment
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

import java.util.{ HashMap => JHashMap }

import com.johnsnowlabs.nlp

import com.google.gson.Gson
import org.apache.spark.ml.evaluation.RegressionEvaluator

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.collection.mutable.WrappedArray

object SAEvaluator {
  /*
   * Reference to Apache Spark regression evaluator
   * as this object is an access wrapper
   */
  private val evaluator = new RegressionEvaluator()
  /*
   * This method calculates proven metric values from
   * the provided prediction by comparing label value
   * and its predicted value
   *
   * - root mean squared error (rsme)
   * - mean squared error (mse)
   * - mean absolute error (mae)
   * - r^2 metric (r2)
   */
  def evaluate(predictions: Dataset[Row], sentimentCol: String, predictionCol: String): String = {
    
    /*
     * Transform sentiment into indices
     */
    val sentiments = predictions.select(sentimentCol).distinct.collect
      .map{case Row(sentiment:String) => sentiment}
      .sorted
      .zipWithIndex
      .map(s => (s._1, s._2.toDouble))
      .toMap
    
    val sentiment2index = sentiment2index_udf(sentiments)
    val indexed = predictions
    .withColumn("_label", sentiment2index(col(sentimentCol)))  
    .withColumn("_prediction", sentiment2index(col(predictionCol)))  
      
    val metrics = new JHashMap[String, Object]()

    evaluator.setLabelCol("_label");
    evaluator.setPredictionCol("_prediction");

    val metricNames = List(
      "rmse",
      "mse",
      "mae",
      "r2")

    metricNames.foreach(metricName => {

      evaluator.setMetricName(metricName)
      val value = evaluator.evaluate(predictions)

      metrics.put(metricName, value.asInstanceOf[AnyRef])

    })

    new Gson().toJson(metrics)

  }
  
  private def sentiment2index_udf(sentMap:Map[String,Double]) = udf{sentiment:String => sentMap(sentiment)}
  
}