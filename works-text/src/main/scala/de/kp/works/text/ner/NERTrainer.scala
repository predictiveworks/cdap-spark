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

import java.util.{Map => JMap}

import com.johnsnowlabs.nlp.annotators.ner.crf.{NerCrfApproach, NerCrfModel}

import org.apache.spark.sql._

import de.kp.works.text.AnnotationBase
import de.kp.works.text.embeddings.Word2VecModel
/*
 * The NER Trainer requires a pre-trained Word2Vec model that 
 * is large enough to cover all tokens of the training corpus.
 * 
 * This NER model trainer expected a corpus in CoNLL format
 */
class NERTrainer(word2vec:Word2VecModel) extends AnnotationBase{

  def train(corpus:Dataset[Row], lineCol:String, params:JMap[String,Object]):NerCrfModel = {    
    /*
     * STEP #1: Transform corpus into annotated dataset;
     * note, the default name of the sentence column is
     * 'column'
     */
    val parser = new CoNLLParser()
    parser.setLineCol(lineCol)

    var document = parser.transform(corpus)
    /*
     * STEP #2: Compute token embeddings
     */
    word2vec.setInputCols(Array("sentence", "token"))
    word2vec.setOutputCol("embeddings")
    
    document = word2vec.transform(document)
    /*
     * STEP #3: Build NER model with the following implicit
     * (default) parameter settings
     * 
     * The L2 regularization coefficient is 1F
     * 
     * The c0 parameter defines decay speed for gradient and is 2250000
     * 
     * The lossEps parameter controls when training is stopped, i.e. when
     * Epoch relative improvement is less than eps
     * 
     * The minW parameter which filters features with less weights then this
     * value is not set
     * 
     */
    val approach = new NerCrfApproach()
    approach.setInputCols("sentence", "token", "pos", "embeddings")
    approach.setLabelColumn("label")
    approach.setOutputCol("ner")

    val minEpochs = if (params.containsKey("minEpochs")) params.get("minEpochs").asInstanceOf[Int] else 0
    approach.setMinEpochs(minEpochs)

    val maxEpochs = if (params.containsKey("maxEpochs")) params.get("maxEpochs").asInstanceOf[Int] else 1000
    approach.setMaxEpochs(maxEpochs)
    
    val model = approach.fit(document)
    model
    
  }
}