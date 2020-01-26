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
import com.johnsnowlabs.nlp.AnnotatorType.{CHUNK, DOCUMENT}
import com.johnsnowlabs.nlp.serialization.ArrayFeature

import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._

import org.apache.spark.sql._

class RegexApproach(override val uid: String) extends AnnotatorApproach[RegexMatcherModel] with HasFeatures {
  
  override val description: String = "Matches described regex rules that come in tuples in a text file"

  override val outputAnnotatorType: AnnotatorType = CHUNK

  override val inputAnnotatorTypes: Array[AnnotatorType] = Array(DOCUMENT)

  val rules: Param[String] = new Param[String](this, "rules", "Delimiter separated list of Regex rules.")

  def setRules(value: String): this.type = set(rules, value)

  def getRules: String = $(rules)

  val strategy: Param[String] = new Param(this, "strategy", "MATCH_ALL|MATCH_FIRST|MATCH_COMPLETE")

  def this() = this(Identifiable.randomUID("REGEX_MATCHER"))

  def setStrategy(value: String): this.type = {
    require(Seq("MATCH_ALL", "MATCH_FIRST", "MATCH_COMPLETE").contains(value.toUpperCase), "Must be MATCH_ALL|MATCH_FIRST|MATCH_COMPLETE")
    set(strategy, value.toUpperCase)
  }

  def getStrategy: String = $(strategy).toString
   
  final val ruleDelimiter = new Param[String](this, "ruleDelimiter",
      "The delimiter to separate individual rules in the ruleset.", (value:String) => true)
  
  def setRuleDelimiter(value:String): this.type = set(ruleDelimiter, value)

  def getRuleDelimiter: String = $(ruleDelimiter).toString

  setDefault(
    inputCols -> Array(DOCUMENT),
    strategy -> "MATCH_ALL",
    ruleDelimiter -> ","
  )

  override def train(dataset: Dataset[_], recursivePipeline: Option[PipelineModel]): RegexMatcherModel = {

    val modelRules = $(rules).split($(ruleDelimiter)).map(rule => (rule.trim, ""))

    new RegexMatcherModel()
      .setRules(modelRules)
      .setStrategy($(strategy))
  
  }

}

object RegexApproach extends DefaultParamsReadable[RegexApproach]
