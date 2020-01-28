package de.kp.works.text.dep
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
import com.johnsnowlabs.nlp.annotators.parser.dep.DependencyParserModel
import org.apache.spark.sql._

class DepTrainer {
  
  def train(dataset:Dataset[Row], lineCol:String, params:JMap[String,Object]):DependencyParserModel = {
    
    val approach = new DepParserApproach()
    approach.setLineCol(lineCol)

    val numIter = if (params.containsKey("numIter")) params.get("numIter").asInstanceOf[Int] else 10
    approach.setNumberOfIterations(numIter)

    val format = if (params.containsKey("format")) params.get("format").asInstanceOf[String] else "conll-u"
    approach.setFormat(format)
    
    approach.fit(dataset)
    
  }

}