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

import org.apache.spark.ml.clustering.{LDA,LDAModel}

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import de.kp.works.text.AnnotationBase
import de.kp.works.text.embeddings.Word2VecModel

/*
 * The LDA Trainer requires a pre-trained Word2Vec model that 
 * is large enough to cover all tokens of the training corpus.
 */
class LDATrainer(word2vec:Word2VecModel) extends AnnotationBase {
  
  def vectorize(dataset:Dataset[Row], textCol:String, params:JMap[String,Object]):Dataset[Row] = {
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
    document.withColumn("vector", embeddings2vector(col("embeddings"))).drop(dropCols: _*)
    
  }
  
  def train(dataset:Dataset[Row], params:JMap[String,Object]):LDAModel = {
    /*
     * Build (Distributed) LDA model
     */
    val model = new LDA()
    
    val k = params.get("k").asInstanceOf[Int]
    model.setK(k)

    val maxIter = params.get("maxIter").asInstanceOf[Int]
    model.setMaxIter(maxIter)
    
    val optimizer = "em"
    model.setOptimizer(optimizer)
    /*
     * The 'alpha' parameter of the Dirichlet prior 
     * on the per-document topic distributions;
     * 
     * it specifies the amount of topic smoothing to 
     * use (> 1.0) (-1 = auto)
     * 
     * the default value '-1' specifies the default 
     * symmetric document-topic prior
     */
    model.setDocConcentration(-1)
    /*
     * The 'beta' parameter of the Dirichlet prior 
     * on the per-topic word distribution;
     * 
     * it specifies the amount of term (word) smoothing 
     * to use (> 1.0) (-1 = auto)
     * 
     * the default value '-1' specifies the default 
     * symmetric topic-word prior
     */      
    model.setTopicConcentration(-1)
    
    model.setFeaturesCol("vector")
    model.fit(dataset)
    
  }
}