package de.kp.works.ml.classification
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
import com.google.gson.Gson

import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql._

object Evaluator {
  /*
   * Reference to Apache Spark classification evaluator
   * as this object is an access wrapper
   */
  private val evaluator = new MulticlassClassificationEvaluator()
    
  /*
   * This method calculates proven metric values from
   * the provided prediction by comparing label value
   * and its predicted value
   * 
   * - accuracy
   * - f1: weighted averaged f-measure
   * - hammingLoss
   * - weightedFMeasure
   * - weightedPrecision
   * - weightedRecall
   * - weightedFalsePositiveRate
   * - weightedTruePositiveRate
   *
	 */  
  def evaluate(predictions: Dataset[Row], labelCol: String, predictionCol: String): String = {

    val metrics = new JHashMap[String, Object]()

    evaluator.setLabelCol(labelCol);
    evaluator.setPredictionCol(predictionCol);

    val metricNames = List(
      "accuracy",      
      "f1",
      "hammingLoss",
      "weightedFMeasure",
      "weightedPrecision",
      "weightedRecall",
      "weightedFalsePositiveRate",
      "weightedTruePositiveRate")
    
    metricNames.foreach(metricName => {

      evaluator.setMetricName(metricName)
      val value = evaluator.evaluate(predictions)

      metrics.put(metricName, value.asInstanceOf[AnyRef])

    })

    new Gson().toJson(metrics)
    
  }

}