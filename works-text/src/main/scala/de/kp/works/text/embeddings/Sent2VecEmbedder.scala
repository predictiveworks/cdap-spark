package de.kp.works.text.embeddings
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
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import de.kp.works.text.AnnotationBase

import scala.collection.mutable.WrappedArray

class Sent2VecEmbedder(model:Word2VecModel) extends AnnotationBase {
  
  def embed(dataset:Dataset[Row], pooling:String, textCol:String, sentenceCol:String, embeddingCol:String, normalization: Boolean):Dataset[Row] = {
    
    var document = extractTokens(dataset, textCol, normalization)
    /*
     * STEP #1: Build token (word) embeddings
     */
    model.setInputCols(Array("document", "token"))
    model.setOutputCol("embeddings")
    
    document = model.transform(document)
    /*
     * STEP #2: Build sentence embedding with provided 
     * pooling strategy
     */
    val embeddings = new com.johnsnowlabs.nlp.embeddings.SentenceEmbeddings()
    embeddings.setPoolingStrategy(pooling)

    embeddings.setInputCols(Array("sentences", "embeddings"))
    embeddings.setOutputCol("embeddings")
    /*
     * The EmbeddingsFinisher does not correctly extract sentences      
     */
    val sentence_udf = udf{sentences:scala.collection.mutable.WrappedArray[Row] => {
      val index = sentences.head.schema.fieldIndex("result")
      sentences.map(sentence => sentence.getString(index))
    }}
       
    document = embeddings.transform(document).withColumn(sentenceCol, sentence_udf(col("sentences")))
    
    val finisher = new com.johnsnowlabs.nlp.EmbeddingsFinisher()
    finisher.setInputCols("embeddings")
    finisher.setOutputCols(embeddingCol)
      
    finisher.transform(document)

  }
}