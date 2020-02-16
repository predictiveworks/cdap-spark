package de.kp.works.text.spell
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
import com.johnsnowlabs.nlp.annotators.spell.norvig._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import de.kp.works.text.AnnotationBase

class NorvigPredictor(model:NorvigSweetingModel) extends AnnotationBase {
  
  def predict(dataset:Dataset[Row], textCol:String, predictionCol:String, threshold:Double):Dataset[Row] = {
    /*
     * The Norvig Corpus is used "AS-IS", i.e. no extra cleaning or
     * normalization steps are performed. However, it is expected
     * that all terms in the corpus are reduced to alphanumeric 
     * characters plus hyphen '-'
     */
    var document = normalizedTokens(dataset, textCol)

    model.setInputCols("token")
    model.setOutputCol("spell")
    
    document = model.transform(dataset)
    /*
     * Spell checking is an instrument for noise reduction; therefore this
     * predictor returns the most likely version of the normalized tokens
     */
    val dropCols = Array("document", "sentences", "token", "spell")
    document
      .withColumn(predictionCol, finishSpellChecker(threshold)(col("token"),col("spell")))
      .drop(dropCols: _*)
    
  }
}