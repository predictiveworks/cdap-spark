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

import java.util.{Map => JMap}

import com.johnsnowlabs.nlp.annotators.LemmatizerModel
import org.apache.spark.sql._

class LemmatizerTrainer {
   
  def train(corpus:Dataset[Row], lineCol:String, params:JMap[String,Object]):LemmatizerModel = {
  
    val approach = new LemmatizerApproach()
    approach.setLineCol(lineCol)
    
    val keyDelimiter = if (params.containsKey("keyDelimiter")) params.get("keyDelimiter").asInstanceOf[String] else "->"
    approach.setKeyDelimiter(keyDelimiter)
    
    val valueDelimiter = if (params.containsKey("valueDelimiter")) params.get("valueDelimiter").asInstanceOf[String] else " "
    approach.setValueDelimiter(valueDelimiter)
    
    approach.fit(corpus)

  }
}