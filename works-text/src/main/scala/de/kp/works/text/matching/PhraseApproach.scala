package de.kp.works.text.matching
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
import com.johnsnowlabs.nlp.annotators._
import com.johnsnowlabs.nlp.AnnotatorType.{CHUNK, DOCUMENT, TOKEN}
import com.johnsnowlabs.nlp.serialization.ArrayFeature

import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._

import org.apache.spark.sql._
/**
 * This class builds a TextMatcherModel from a user specific
 * list of phrases; this is a slightly modified version of
 * Spark-NLP's TextMatcher
 */
class PhraseApproach(override val uid: String) extends AnnotatorApproach[TextMatcherModel] with HasFeatures {

  def this() = this(Identifiable.randomUID("PHRASE_EXTRACTOR"))

  override val inputAnnotatorTypes = Array(DOCUMENT, TOKEN)

  override val outputAnnotatorType: AnnotatorType = CHUNK

  override val description: String = "Extracts given phrases from target dataset"

  val caseSensitive = new BooleanParam(this, "caseSensitive", "whether to match regardless of case. Defaults true")
 
  def setCaseSensitive(v: Boolean): this.type =
    set(caseSensitive, v)

  def getCaseSensitive: Boolean =
    $(caseSensitive)

  val phrases: Param[String]  = new Param[String](this, "phrases", "Delimiter separated list of phrases")

  def setPhrases(value: String): this.type = set(phrases, value)

  def getPhrases: String = $(phrases)
   
  final val phraseDelimiter = new Param[String](this, "phraseDelimiter",
      "The delimiter to separate individual phrases in the set of phrases.", (value:String) => true)
  
  def setPhraseDelimiter(value:String): this.type = set(phraseDelimiter, value)

  def getPhraseDelimiter: String = $(phraseDelimiter).toString

  override def train(dataset: Dataset[_], recursivePipeline: Option[PipelineModel]):TextMatcherModel = {
    
    val tokenized = $(phrases).split($(phraseDelimiter)).map(line => line.trim.split(" ").map(_.trim))
    
    new TextMatcherModel()
      .setEntities(tokenized)
      .setCaseSensitive($(caseSensitive))
    
  }

}

object PhraseApproach extends DefaultParamsReadable[PhraseApproach]
