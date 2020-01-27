package de.kp.works.text.lemma
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
import com.johnsnowlabs.nlp.annotators.LemmatizerModel

import org.apache.spark.sql._
import de.kp.works.text.AnnotationBase

class LemmaPredictor(model:LemmatizerModel) extends AnnotationBase {
  
  def predict(dataset:Dataset[Row], textCol:String, tokenCol:String, predictionCol:String):Dataset[Row] = {
    
    val document = normalizedTokens(dataset, textCol)

    model.setInputCols("token")
    model.setOutputCol("lemma")
    
    val lemmatized = model.transform(dataset)
    
    val finisher = new com.johnsnowlabs.nlp.Finisher()
    .setInputCols(Array("token", "lemma"))
    .setOutputCols(Array(tokenCol,predictionCol))

    finisher.transform(lemmatized)

  }
}