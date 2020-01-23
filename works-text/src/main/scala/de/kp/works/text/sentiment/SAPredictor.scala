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

import scala.collection.mutable.WrappedArray

class SAPredictor(model:nlp.annotators.sda.vivekn.ViveknSentimentModel) {
  
  def predict(dataset:Dataset[Row], textCol:String, predictionCol:String):Dataset[Row] = {
    
    /**** PREPARATION STEPS ****/
    
    /*
     * The initial step is defined by the DocumentAssembler which generates
     * an initial annotation; note, as we leverage an internal pipeline to
     * predict the sentiment, we do not have to serialize / deserialize
     * the respective annotations
     */
    val documentAssembler = new nlp.DocumentAssembler()
    documentAssembler.setInputCol(textCol)
    documentAssembler.setOutputCol("_document")
    
    val document = documentAssembler.transform(dataset)
    /*
     * We expect that the provided text can define multiple sentences;
     * therefore an additional sentence detection stage is used.
     */
		val sentenceDetector = new nlp.annotators.sbd.pragmatic.SentenceDetector()
    sentenceDetector.setInputCols("_document")
    sentenceDetector.setOutputCol("_sentence")   
    
    val sentence = sentenceDetector.transform(document)
    /*
     * Next the tokenizer is applied to transform each detected
     * sentence into a set of tokens
     */
    val tokenizer = new nlp.annotators.Tokenizer()
    tokenizer.setInputCols("_sentence")
    tokenizer.setOutputCol("_token")
    
    val tokenized = tokenizer.fit(sentence).transform(sentence)
    /*
     * Before applying the sentiment approach of Vivek Narayanan,
     * we normalize the extracted tokens
     */
    val normalizer = new nlp.annotators.Normalizer()
    normalizer.setInputCols("_token")
    normalizer.setOutputCol("_normal")
    
    val normalized = normalizer.fit(tokenized).transform(tokenized)
    
    /**** PREDICTION ****/
    
   /*
     * The columns returned by the model are annotation
     * columns with a Spark-NLP specific format
     */
    model.setOutputCol("_sentiment")
    val predicted = model.transform(normalized)
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