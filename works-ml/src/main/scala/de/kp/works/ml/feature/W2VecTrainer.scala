package de.kp.works.ml.feature
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

import org.apache.spark.ml.feature._
import org.apache.spark.sql._

class W2VecTrainer {
      
  def train(dataset:Dataset[Row], inputCol:String, params:JMap[String,Object]):Word2VecModel = {
    
    val words2vec = new Word2Vec()

    val maxIter = params.get("maxIter").asInstanceOf[Int]
    words2vec.setMaxIter(maxIter)
    
    val stepSize = params.get("stepSize").asInstanceOf[Double]
    words2vec.setStepSize(stepSize)
    
    val vectorSize = params.get("vectorSize").asInstanceOf[Int]
    words2vec.setVectorSize(vectorSize)
    
    val windowSize = params.get("windowSize").asInstanceOf[Int]
    words2vec.setWindowSize(windowSize)
    
    val minCount = params.get("minCount").asInstanceOf[Int]
    words2vec.setMinCount(minCount)
    
    val maxSentenceLength = params.get("maxSentenceLength").asInstanceOf[Int]
    words2vec.setMaxSentenceLength(maxSentenceLength)
    
    words2vec.setInputCol(inputCol)
    words2vec.fit(dataset)

  }
    

}