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
import com.johnsnowlabs.nlp._
import com.johnsnowlabs.nlp.annotators.spell.norvig._

import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.param._

import org.apache.spark.ml.util._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.mutable.{Map => MMap, WrappedArray}

class NorvigApproach(override val uid: String) extends AnnotatorApproach[NorvigSweetingModel]
    with NorvigSweetingParams {

  import com.johnsnowlabs.nlp.AnnotatorType._

  override val description: String = "Spell checking algorithm inspired on Norvig model"

  override val outputAnnotatorType: AnnotatorType = TOKEN

  override val inputAnnotatorTypes: Array[AnnotatorType] = Array(TOKEN)

  def this() = this(Identifiable.randomUID("SPELL"))
   
  final val termsCol = new Param[String](this, "termsol",
      "Name of the input text field that contains terms of a training corpus.", (value:String) => true)

  def setTermsCol(value:String): this.type = set(termsCol, value)
  setDefault(termsCol -> "terms")
   
  final val tokenPattern = new Param[String](this, "tokenPattern",
      "Regex pattern to separate terms. Defaults is \\S+.", (value:String) => true)

  def setTokenPattern(value:String): this.type = set(tokenPattern, value)
  setDefault(tokenPattern -> "\\S+")
 
  setDefault(
    caseSensitive -> true,
    doubleVariants -> false,
    shortCircuit -> false,
    frequencyPriority -> true,
    wordSizeIgnore -> 3,
    /* 
     * Note: This parameters controls the number of duplicates, i.e. Pe(e)ter is 
     * a term with one duplicate and Pe(ee)ter a term with two duplicates
     *  
     */
    dupsLimit -> 1,
    reductLimit -> 3,
    intersections -> 10,
    vowelSwapLimit -> 6
  )

  override def train(corpus: Dataset[_], recursivePipeline: Option[PipelineModel]): NorvigSweetingModel = {
    
    val lines = corpus.select($(termsCol)).collect.map{case Row(line:String) => line}

    val regex = $(tokenPattern).r
    val wordCount: MMap[String, Long] = MMap.empty[String, Long].withDefaultValue(0)
    
    lines.foreach(line => {
 
      val words: List[String] = regex.findAllMatchIn(line).map(_.matched).toList
      words.foreach(w => wordCount(w) += 1)
      
    })
        
    if (wordCount.isEmpty)
      throw new IllegalArgumentException("Word count dictionary for spell checker does not exist or is empty.")
    
    new NorvigSweetingModel()
      .setWordSizeIgnore($(wordSizeIgnore))
      .setDupsLimit($(dupsLimit))
      .setReductLimit($(reductLimit))
      .setIntersections($(intersections))
      .setVowelSwapLimit($(vowelSwapLimit))
      .setWordCount(wordCount.toMap)
      .setDoubleVariants($(doubleVariants))
      .setCaseSensitive($(caseSensitive))
      .setShortCircuit($(shortCircuit))
      .setFrequencyPriority($(frequencyPriority))
   
  }
 
}

object NorvigApproach extends DefaultParamsReadable[NorvigApproach]