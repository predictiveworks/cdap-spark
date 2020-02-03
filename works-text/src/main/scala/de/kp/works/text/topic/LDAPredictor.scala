package de.kp.works.text.topic
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

import java.util.{Map => JMap}

import org.apache.spark.ml.clustering.LDAModel
import org.apache.spark.ml.linalg.Vector

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import de.kp.works.text.AnnotationBase
import de.kp.works.text.embeddings.Word2VecModel

/*
 * In contrast to say, KMeans, where each entity can only belong to a single cluster
 * (hrad-clustering), LDA allows to interpret the topic probabilities as the proportion
 * of cluster membership (soft-clustering), or 'fuzzy clustering'.
 * 
 * This provides a more nuanced way of recommending similar items, finding duplicates,
 * or discovering user profiles/personas.
 * 
 * LDALabeler supports use cases where hard-clustering based on LDA is required.
 */
class LDALabeler(model:LDAModel, word2vec:Word2VecModel) extends AnnotationBase {
  
  def label(dataset:Dataset[Row], textCol:String, labelCol:String, params:JMap[String,Object]):Dataset[Row] = {
    /*
     * STEP #1: Leverage the LDA predictor to determine
     * the topic distribution
     */
    val predictions = new LDAPredictor(model, word2vec).predict(dataset, textCol, "topics", params)
    /*
     * Determine the index of the topic with the highest
     * contribution to the document
     */
    val topics2label = udf{topics:Vector => {
      
      val zipped = topics.toArray.zipWithIndex
      val label = zipped.sortBy(- _._1).head._1
      
      label.toDouble
    }}

    val output = predictions.withColumn(labelCol, topics2label(col("topics")))
    output.drop("topics")

  }
  
}

/*
 * The LDAPredictor is used to assign a certain label distribution (can be intepreted as
 * topics) to a text document. The label distribution is computed by leveraging a pre-
 * trained LDA model based on doument embeddings 
 */
class LDAPredictor(model:LDAModel, word2vec:Word2VecModel) extends AnnotationBase {
   
  def predict(dataset:Dataset[Row], textCol:String, topicCol:String, params:JMap[String,Object]):Dataset[Row] = {
    /*
     * STEP #1: Extracted normalized tokens and transform
     * each token into an embedding vector
     */
    var document = normalizedTokens(dataset, textCol)

    word2vec.setInputCols(Array("sentences", "token"))
    word2vec.setOutputCol("embeddings")
    
    document = word2vec.transform(document)
    /*
     * STEP #2: Transform token embeddings into a document
     * feature vector
     */
    val strategy = if (params.containsKey("strategy")) params.get("strategy").asInstanceOf[String] else "AVERAGE"
    val embeddings2vector = embeddings2vector_udf(strategy)
    
    val dropCols = Array("document", "sentences", "token", "embeddings")
    document = document.withColumn("vector", embeddings2vector(col("embeddings"))).drop(dropCols: _*)
    
    model.setFeaturesCol("vector")
    /*
     * The model adds an internal column "topicDistribution" 
     * to the document; this is a k-dimensional vector that
     * determines the portion each topic contributes to the
     * document (line)  
     */
    document = model.transform(document)
      .withColumnRenamed("topicDistribution", topicCol).drop("vector")

    document
    
  }
  
  def devectorize(dataset:Dataset[Row], vectorCol:String, featureCol:String): Dataset[Row] = {
    /*
     * The dataset contains an Apache Spark Vector and must be transformed
     * into an Array of Double value (to CDAP Structured Record)
     */
    val devector_udf = udf {vector:Vector => {
      vector.toArray
    }}

    dataset.withColumn(featureCol, devector_udf(col(vectorCol)))
    
  }
 
}