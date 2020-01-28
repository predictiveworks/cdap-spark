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
import com.johnsnowlabs.nlp.annotators.parser.dep.GreedyTransition.WordData

import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.util._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

case class CoNLLUSentence(words:List[WordData])

trait CoNLLUParams extends Params {
   
  final val lineCol = new Param[String](CoNLLUParams.this, "lineCol",
      "Name of the input text field that contains the annotated sentences for training purpose.", (value:String) => true)

  def setLineCol(value:String): this.type = set(lineCol, value)
  setDefault(lineCol -> "line")
  
  def validateSchema(schema:StructType):Unit = {
    
    /* LINE FIELD */
    
    val lineColName = $(lineCol)  
    
    if (schema.fieldNames.contains(lineColName) == false)
      throw new IllegalArgumentException(s"Input column $lineColName does not exist.")
    
    val lineColType = schema(lineColName).dataType
    if (!(lineColType == StringType)) {
      throw new IllegalArgumentException(s"Data type of input column $lineColName must be StringType.")
    }
    
  }

}

class CoNLLUParser(override val uid: String) extends Transformer with CoNLLUParams {

  def this() = this(Identifiable.randomUID("coNLLUParser"))

  def transform(dataset: Dataset[_]): DataFrame = {

    validateSchema(dataset.schema)

    val lines = dataset.select($(lineCol)).collect.map{case Row(line:String) => line}
    val sentences = getTrainingSentencesFromConllU(lines)

    import dataset.sparkSession.implicits._
    sentences.toDF("sentence")
  
  }
  /*
   * This method is copied from Spark-NLP [DependencyParserApproach]
   */
  private def getTrainingSentencesFromConllU(conllUAsArray: Array[String]): List[CoNLLUSentence] = {

    val conllUSentences = conllUAsArray.filterNot(line => lineIsComment(line))
    val indexSentenceBoundaries = conllUSentences.zipWithIndex.filter(_._1 == "").map(_._2)
    val cleanConllUSentences = indexSentenceBoundaries.zipWithIndex.map{case (indexSentenceBoundary, index) =>
      if (index == 0){
        conllUSentences.slice(index, indexSentenceBoundary)
      } else {
        conllUSentences.slice(indexSentenceBoundaries(index-1)+1, indexSentenceBoundary)
      }
    }
    val sentences = cleanConllUSentences.map{cleanConllUSentence =>
      transformToSentences(cleanConllUSentence)
    }
    sentences.map(words => CoNLLUSentence(words)).toList
  }
  /*
   * This method is copied from Spark-NLP [DependencyParserApproach]
   */
  private def transformToSentences(cleanConllUSentence: Array[String]): List[WordData] = {
    val ID_INDEX = 0
    val WORD_INDEX = 1
    val POS_INDEX = 4
    val HEAD_INDEX = 6
    val SEPARATOR = "\\t"

    val sentences = cleanConllUSentence.map{conllUWord =>
      val wordArray = conllUWord.split(SEPARATOR)
      if (!wordArray(ID_INDEX).contains(".")){
        var head = wordArray(HEAD_INDEX).toInt
        if (head == 0){
          head = cleanConllUSentence.length
        } else {
          head = head-1
        }
        WordData(wordArray(WORD_INDEX), wordArray(POS_INDEX), head)
      } else {
        WordData("", "", -1)
      }
    }

    sentences.filter(word => word.dep != -1).toList

  }

  /*
   * This method is copied from Spark-NLP [DependencyParserApproach]
   */
  private def lineIsComment(line: String): Boolean = {
    if (line.nonEmpty){
      line(0) == '#'
    } else {
      false
    }
  }

  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  override def copy(extra: ParamMap): CoNLLUParser = defaultCopy(extra)
  
}