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
import org.apache.spark.sql.types._

class SATrainer {
  /*
   * This training phase is based on the Vivekn sentiment analysis approach:
   * 
   * see https://arxiv.org/abs/1305.6143
   * 
   * Fast and accurate sentiment classification using an enhanced Naive Bayes model
   * 
   * Enhancement refers to the methods of improving the accuracy of a Naive Bayes 
   * classifier for sentiment analysis. The approach is based on a combination of 
   * methods like negation handling, word n-grams and feature selection by mutual 
   * information results in a significant improvement in accuracy. 
   * 
   * This implies that a highly accurate and fast sentiment classifier can be built 
   * using a simple Naive Bayes model that has linear training and testing time 
   * complexities. 
   * 
   * Original accuracy: 88.80% on the popular IMDB movie reviews dataset.
   * 
   */
  def train(trainset:Dataset[_], textCol:String, sentimentCol:String): nlp.annotators.sda.vivekn.ViveknSentimentModel = {
    /*
     * The dataset contains at least two columns, one that contains a certain
     * sample document, and another which holds the assigned sentiment.
     * 
     * The initial step is defined by the DocumentAssembler which generates
     * an initial annotation; note, as we leverage an internal pipeline to
     * train the sentiment model, we do not have to serialize / deserialize
     * the respective annotations
     */
    val documentAssembler = new nlp.DocumentAssembler()
    documentAssembler.setInputCol(textCol)
    documentAssembler.setOutputCol("_document")
    
    val document = documentAssembler.transform(trainset)
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
    
    val algorithm = new nlp.annotators.sda.vivekn.ViveknSentimentApproach()
    .setInputCols("_document", "_normal")
    .setSentimentCol(sentimentCol)
    
    val model = algorithm.fit(normalized)
    model

  }
}

object SentimentTrainer {
  
  def main(args:Array[String]) {
 
    val session = SparkSession.builder
      .appName("SentimentTrainer")
      .master("local")
      .getOrCreate()

    val training = Seq(
      Row("I really liked this movie!", "positive"),
      Row("The cast was horrible", "negative"),
      Row("Never going to watch this again or recommend it to anyone", "negative"),
      Row("It's a waste of time", "negative"),
      Row("I loved the protagonist", "positive"),
      Row("The music was really really good", "positive")
    )

     
    val schema = StructType(Array(
      StructField("text", StringType, true),
      StructField("sentiment", StringType, true)
    ))
     
    val ds = session.createDataFrame(session.sparkContext.parallelize(training), schema)
   
    val trainer = new SATrainer()
    val model = trainer.train(ds, "text", "sentiment")

    val loaded = nlp.annotators.sda.vivekn.ViveknSentimentModel.load("")    
  }
}