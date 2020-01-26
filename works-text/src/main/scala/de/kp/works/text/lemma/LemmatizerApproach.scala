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

import com.johnsnowlabs.nlp._
import com.johnsnowlabs.nlp.annotators.LemmatizerModel

import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.param._

import org.apache.spark.ml.util._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.mutable.{Map => MMap, WrappedArray}

class LemmatizerApproach(override val uid: String) extends AnnotatorApproach[LemmatizerModel] {

  import com.johnsnowlabs.nlp.AnnotatorType._

  override val description: String = "Retrieves the significant part of a word"

  override val outputAnnotatorType: AnnotatorType = TOKEN

  override val inputAnnotatorTypes: Array[AnnotatorType] = Array(TOKEN)

  def this() = this(Identifiable.randomUID("LEMMATIZER"))
   
  final val lineCol = new Param[String](this, "lineCol",
      "Name of the input text field that contains lemma and associated tokens.", (value:String) => true)
   
  final val keyDelimiter = new Param[String](this, "keyDelimiter",
      "The delimiter to separate 'lemma' and associated tokens in the corpus.", (value:String) => true)
   
  final val valueDelimiter = new Param[String](this, "valueDelimiter",
      "The delimiter to separate the tokens in the dictionary.", (value:String) => true)

  def setLineCol(value:String): this.type = set(lineCol, value)
  setDefault(lineCol -> "line")
  
  def setKeyDelimiter(value:String): this.type = set(keyDelimiter, value)
  setDefault(keyDelimiter -> "->")
 
  def setValueDelimiter(value:String): this.type = set(valueDelimiter, value)
  setDefault(valueDelimiter -> " ")

  /*
   * The lemma corpus supported by Spark NLP is defined by the
   * folling format: lemma -> token, token token
   */
  override def train(corpus: Dataset[_], recursivePipeline: Option[PipelineModel]): LemmatizerModel = {
    
    val lines = corpus.select($(lineCol)).collect.map{case Row(line:String) => line}
    
    val m: MMap[String, String] = MMap()
    lines.foreach(line => {

      val kv = line.split($(keyDelimiter)).map(_.trim)
      
      val key = kv(0)
      val values = kv(1).split($(valueDelimiter)).map(_.trim)
      /*
       * Invert struct and leverage token as key
       */
      values.foreach(m(_) = key)
      
    })
    
    new LemmatizerModel().setLemmaDict(m.toMap)

  }
}

object LemmatizerApproach extends DefaultParamsReadable[LemmatizerApproach]
