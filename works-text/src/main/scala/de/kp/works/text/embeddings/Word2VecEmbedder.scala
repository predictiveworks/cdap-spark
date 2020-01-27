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
import de.kp.works.text.AnnotationBase

class Word2VecEmbedder(model:Word2VecModel) extends AnnotationBase {
  
  def embed(dataset:Dataset[Row], textCol:String, tokenCol:String, embeddingCol:String):Dataset[Row] = {
    
    val document = normalizedTokens(dataset, textCol)

    model.setInputCols(Array("document", "token"))
    model.setOutputCol("embeddings")
    
    val embedded = model.transform(document)
       
    val finisher = new com.johnsnowlabs.nlp.EmbeddingsFinisher()
    finisher.setInputCols(Array("token","embeddings"))
    finisher.setOutputCols(Array(tokenCol, embeddingCol))
      
    finisher.transform(embedded)

  }
}