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

import com.johnsnowlabs.nlp

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import de.kp.works.text.AnnotationBase
import scala.collection.mutable.WrappedArray

class SAPredictor(model:nlp.annotators.sda.vivekn.ViveknSentimentModel) extends AnnotationBase {
  
  def predict(dataset:Dataset[Row], textCol:String, predictionCol:String):Dataset[Row] = {
    
    val document = normalizedTokens(dataset, textCol)
    /*
     * The columns returned by the model are annotation
     * columns with a Spark-NLP specific format
     */
    model.setInputCols(Array("document", "token"))
    model.setOutputCol("_sentiment")

    val predicted = model.transform(document)
    /*
     * As a final step, we leverage the Spark-NLP finisher
     * to remove all the respective internal annotations
     */
    val finisher = new nlp.Finisher()
    .setInputCols("_sentiment")
    .setOutputCols("_predicted")

    val finished = finisher.transform(predicted)
    /*
     * The output column of the finisher is an Array[String]
     * which has to be transformed into a [String]
     */
    val text_udf = udf{array:WrappedArray[String] => array.head}    
    finished.withColumn(predictionCol, text_udf(col("_predicted"))).drop("_predicted")
    
  }

}