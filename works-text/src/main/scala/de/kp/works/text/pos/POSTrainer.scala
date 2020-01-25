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

import java.util.{Map => JMap}

import com.johnsnowlabs.nlp.annotators.pos.perceptron.{PerceptronApproach, PerceptronModel}
import org.apache.spark.sql._

class POSTrainer {

  def train(dataset:Dataset[Row], lineCol:String, params:JMap[String,Object]):PerceptronModel = {
    
    val parser = new POSParser()
    parser.setLineCol(lineCol)

    val parsed = parser.transform(dataset)

    val approach = new PerceptronApproach()

    val maxIter = if (params.containsKey("maxIter")) params.get("maxIter").asInstanceOf[Int] else 5    
    approach.setNIterations(maxIter)
    
    approach.setInputCols(Array("document", "token"))
    approach.setPosColumn("pos")
    approach.setOutputCol("tags")
    
    approach.fit(parsed)
    
  }
  
}