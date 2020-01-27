package de.kp.works.text.ner
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

import com.johnsnowlabs.nlp.annotators.ner.crf.NerCrfModel

import de.kp.works.text.AnnotationBase
import de.kp.works.text.embeddings.Word2VecModel

import org.apache.spark.sql._

class NERPredictor(nercrf:NerCrfModel, word2vec:Word2VecModel) extends AnnotationBase {
  
  def predict(dataset:Dataset[Row], textCol:String, tokenCol:String, nerCol:String):Dataset[Row] = {
    
    var document = normalizedTokens(dataset, textCol)
    /*
     * 'sentences' is the column build by the 'normalizedTokens'
     * method is named slightly different from the CoNLL Parser
     */
    word2vec.setInputCols(Array("sentences", "token"))
    word2vec.setOutputCol("embeddings")
    
    document = word2vec.transform(document)
    
    nercrf.setInputCols("sentences", "token", "pos", "embeddings")
    nercrf.setOutputCol("ner")

    document = nercrf.transform(document)

    val finisher = new com.johnsnowlabs.nlp.Finisher()
    .setInputCols(Array("token","ner"))
    .setOutputCols(Array(tokenCol, nerCol))
    
    finisher.transform(document)

  }
}