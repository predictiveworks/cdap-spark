package de.kp.works.text.pos
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
import com.johnsnowlabs.nlp.annotators.pos.perceptron.PerceptronModel

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import de.kp.works.text.AnnotationBase

class POSPredictor(model:PerceptronModel) extends AnnotationBase {
  
  def predict(dataset:Dataset[Row], textCol:String, mixinCol:String):Dataset[Row] = {
    
    var document = normalizedTokens(dataset, textCol)
    
    model.setInputCols(Array("sentences", "token"))
    model.setOutputCol("tags")
    
    document = model.transform(document)
    /*
     * POS tagging is a means to build strong text features as the
     * linguistic context of a word is preserved; therefore token
     * and detected tag are concatenated into a string word.
     */
    val dropCols = Array("document", "sentences", "token", "tags")
    document
      .withColumn(mixinCol, finishPOSTagger(col("token"), col("tags")))
      .drop(dropCols: _*)
     
  }
  
}