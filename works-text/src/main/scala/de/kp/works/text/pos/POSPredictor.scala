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
import de.kp.works.text.AnnotationBase

class POSPredictor(model:PerceptronModel) extends AnnotationBase {
  
  def predict(dataset:Dataset[Row], textCol:String, tokenCol:String, predictionCol:String):Dataset[Row] = {
    
    val document = normalizedTokens(dataset, textCol)
    
    model.setInputCols(Array("sentence", "token"))
    model.setOutputCol("tags")
    
    val tagged = model.transform(dataset)
    
    val finisher = new com.johnsnowlabs.nlp.Finisher()
    .setInputCols(Array("token", "tags"))
    .setOutputCols(Array(tokenCol, predictionCol))

    finisher.transform(tagged)
    
  }
  
}