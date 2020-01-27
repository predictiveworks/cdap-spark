package de.kp.works.text.embeddings
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

import com.johnsnowlabs.nlp
import org.apache.spark.sql._

import de.kp.works.text.AnnotationBase

class Word2VecTrainer extends AnnotationBase {

  def train(dataset:Dataset[Row], textCol:String, params:JMap[String,Object]):Word2VecModel = {
    
    val document = normalizedTokens(dataset, textCol)

    val approach = new Word2VecApproach()
		
		val vectorSize = if (params.containsKey("vectorSize")) params.get("vectorSize").asInstanceOf[Int] else 100
		approach.setVectorSize(vectorSize)
		
		val windowSize = if (params.containsKey("windowSize")) params.get("windowSize").asInstanceOf[Int] else 5
		approach.setWindowSize(windowSize)
		
		val minCount = if (params.containsKey("minCount")) params.get("minCount").asInstanceOf[Int] else 1
		approach.setMinCount(minCount)
		
		val maxSentenceLength = if (params.containsKey("maxSentenceLength")) params.get("maxSentenceLength").asInstanceOf[Int] else 1000
		approach.setMaxSentenceLength(maxSentenceLength)
		
		val maxIter = if (params.containsKey("maxIter")) params.get("maxIter").asInstanceOf[Int] else 1
		approach.setMaxIter(maxIter)
		
		val stepSize = if (params.containsKey("stepSize")) params.get("stepSize").asInstanceOf[Double] else 0.025
		approach.setStepSize(stepSize)
    
		approach.setInputCols(Array("sentences", "token"))
		approach.setOutputCol("embeddings")
    
    approach.fit(document)
    
  }
}