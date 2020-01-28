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
import com.johnsnowlabs.nlp.AnnotatorApproach
import com.johnsnowlabs.nlp.AnnotatorType._
import com.johnsnowlabs.nlp.annotators.parser.dep._
import com.johnsnowlabs.nlp.annotators.parser.dep.GreedyTransition._

import org.apache.spark.ml.PipelineModel

import org.apache.spark.ml.param._
import org.apache.spark.ml.util._

import org.apache.spark.sql._
import scala.collection.mutable.WrappedArray

class DepParserApproach(override val uid: String) extends AnnotatorApproach[DependencyParserModel] {

  override val description: String =
    "Dependency Parser is an unlabeled parser that finds a grammatical relation between two words in a sentence"

  def this() = this(Identifiable.randomUID("depParserApproach"))

  override val outputAnnotatorType:String = DEPENDENCY

  override val inputAnnotatorTypes = Array(DOCUMENT, POS, TOKEN)

  val format: Param[String] = {
    val allowedParams = ParamValidators.inArray(Array("conll-u", "treebank"))
    new Param(
      this, "format", "The format of the training corpus. Supported values are 'conll-u' (CoNNL-u corpus) and 'treebank' (TreeBank corpus).", allowedParams)
  }

  def setFormat(value:String): this.type = set(format, value)
  setDefault(format -> "conll-u")
   
  final val lineCol = new Param[String](this, "lineCol",
      "Name of the input text field that contains the annotated sentences for training purpose.", (value:String) => true)

  def setLineCol(value:String): this.type = set(lineCol, value)
  setDefault(lineCol -> "line")
  
  val numberOfIterations = new IntParam(this, "numberOfIterations", "Number of iterations in training, converges to better accuracy")

  def setNumberOfIterations(value: Int): this.type = set(numberOfIterations, value)
  setDefault(numberOfIterations, 10)

  override def train(dataset: Dataset[_], recursivePipeline: Option[PipelineModel]): DependencyParserModel = {
 
    /* Transform corpus into Spark-NLP [Sentence] format */
    val sentences = getSentences(dataset)

    val (classes, tagDictionary) = TagDictionary.classesAndTagDictionary(sentences)
    val tagger = new Tagger(classes, tagDictionary)

    val taggerNumberOfIterations = $(numberOfIterations)

    val dependencyMaker = new DependencyMaker(tagger)

    val dependencyMakerPerformanceProgress = (0 until taggerNumberOfIterations).map{ seed =>
      dependencyMaker.train(sentences, seed)
    }

    new DependencyParserModel()
      .setPerceptron(dependencyMaker)

  }
  
  private def getSentences(dataset:Dataset[_]):List[Sentence] = {
   
    /*
     * Read provided dataset in a common format independent
     * of the original 'conll-u' or 'treebank' format
     */
    val corpus = if ($(format) == "conll-u") {

      val parser = new CoNLLUParser()
      parser.setLineCol($(lineCol))

      parser.transform(dataset)
      
    } else {

      val parser = new TreeBankParser()
      parser.setLineCol($(lineCol))

      parser.transform(dataset)
      
      
    }
    /* Transform corpus into Spark-NLP [Sentence] format */
    val sentences = corpus.select("sentence").collect.map(row => {

      row.getAs[WrappedArray[Row]](0).map(item => {
        WordData(raw = item.getString(0), pos = item.getString(1), dep = item.getInt(2))
      }).toList

    }).toList
    
    sentences
    
  }
  
}

object DepParserApproach extends DefaultParamsReadable[DepParserApproach]