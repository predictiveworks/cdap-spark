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

import java.util.{Map => JMap}

import com.johnsnowlabs.nlp.annotators.spell.norvig._
import org.apache.spark.sql._

class NorvigTrainer {
 
  def train(corpus:Dataset[Row], termsCol:String, params:JMap[String,Object]):NorvigSweetingModel = {
  
    val approach = new NorvigApproach()
    approach.setTermsCol(termsCol)
    
    val tokenPattern = if (params.containsKey("tokenPattern")) params.get("tokenPattern").asInstanceOf[String] else "\\S+"
    approach.setTokenPattern(tokenPattern)
    
    val caseSensitive = if (params.containsKey("caseSensitive")) params.get("caseSensitive").asInstanceOf[Boolean] else true
    approach.setCaseSensitive(caseSensitive)
    
    val doubleVariants = if (params.containsKey("doubleVariants")) params.get("doubleVariants").asInstanceOf[Boolean] else false
    approach.setDoubleVariants(doubleVariants)
    
    val shortCircuit = if (params.containsKey("shortCircuit")) params.get("shortCircuit").asInstanceOf[Boolean] else false
    approach.setShortCircuit(shortCircuit)
    
    val frequencyPriority = if (params.containsKey("frequencyPriority")) params.get("frequencyPriority").asInstanceOf[Boolean] else true
    approach.setFrequencyPriority(frequencyPriority)

    val wordSizeIgnore = if (params.containsKey("wordSizeIgnore")) params.get("wordSizeIgnore").asInstanceOf[Int] else 3
    approach.setWordSizeIgnore(wordSizeIgnore)

    val dupsLimit = if (params.containsKey("dupsLimit")) params.get("dupsLimit").asInstanceOf[Int] else 2
    approach.setDupsLimit(dupsLimit)

    val reductLimit = if (params.containsKey("reductLimit")) params.get("reductLimit").asInstanceOf[Int] else 3
    approach.setReductLimit(reductLimit)

    val intersections = if (params.containsKey("intersections")) params.get("intersections").asInstanceOf[Int] else 10
    approach.setIntersections(intersections)

    val vowelSwapLimit = if (params.containsKey("vowelSwapLimit")) params.get("vowelSwapLimit").asInstanceOf[Int] else 6
    approach.setVowelSwapLimit(vowelSwapLimit)

    approach.fit(corpus)
    
  }
  
}