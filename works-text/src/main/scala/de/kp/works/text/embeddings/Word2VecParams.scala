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

import com.johnsnowlabs.nlp.AnnotatorType

import org.apache.spark.ml.param._

import org.apache.spark.sql.Column
import org.apache.spark.sql.types.MetadataBuilder
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
import org.apache.spark.ml.param._

trait Word2VecParams extends Params {
   
  final val embeddingsCol = new Param[String](this, "embeddingsCol",
      "Name of the output field that contains the annotated embeddings.", (value:String) => true)

  def setEmbeddingsCol(value:String): this.type = {
    set(this.embeddingsCol, value)
  }
  
  setDefault(embeddingsCol -> "embeddings")
  
  /**
   * The dimension of the code that you want to transform from words. Default: 100
   */
  final val vectorSize = new IntParam(
    this, "vectorSize", "the dimension of codes after transforming from words (> 0). Default is 100.",
    ParamValidators.gt(0))

  def setVectorSize(value:Int = 100): this.type = {
    set(this.vectorSize, value)
  }

  def getVectorSize:Int = $(vectorSize)
  
  /**
   * The window size (context words from [-window, window]). Default: 5
   */
  final val windowSize = new IntParam(
    this, "windowSize", "the window size (context words from [-window, window]) (> 0). Default is 5.",
    ParamValidators.gt(0))

  def setWindowSize(value:Int = 5): this.type = {
    set(this.windowSize, value)
  }

  /**
   * The minimum number of times a token must appear to be included in the word2vec model's
   * vocabulary. Default: 5
   */
  final val minCount = new IntParam(this, "minCount", "the minimum number of times a token must " +
    "appear to be included in the word2vec model's vocabulary (>= 0). Default is 1.", ParamValidators.gtEq(0))

  def setMinCount(value:Int = 1): this.type = {
    set(this.minCount, value)
  }

  /**
   * Sets the maximum length (in words) of each sentence in the input data.
   * Any sentence longer than this threshold will be divided into chunks of
   * up to `maxSentenceLength` size. Default is 1000.
   */
  final val maxSentenceLength = new IntParam(this, "maxSentenceLength", "Maximum length " +
    "(in words) of each sentence in the input data. Any sentence longer than this threshold will " +
    "be divided into chunks up to the size (> 0). Default is 1000.", ParamValidators.gt(0))

  def setMaxSentenceLength(value:Int = 1000): this.type = {
    set(this.maxSentenceLength, value)
  }
  
  /**
   * Param for maximum number of iterations (&gt;= 0). Default is 1.
   */
  final val maxIter: IntParam = new IntParam(this, "maxIter", 
      "maximum number of iterations (>= 0). Default is 1.", ParamValidators.gtEq(0))

  def setMaxIter(value:Int = 1): this.type = {
    set(this.maxIter, value)
  }

  /**
   * Param for Step size to be used for each iteration of optimization (&gt; 0). Default is 0.025.
   */
  val stepSize: DoubleParam = new DoubleParam(this, "stepSize", 
      "Step size to be used for each iteration of optimization (> 0). Default is 0.025.", ParamValidators.gt(0))

  def setStepSize(value:Double = 0.025): this.type = {
    set(this.stepSize, value)
  }
  

  setDefault(vectorSize -> 100, windowSize -> 5, minCount -> 1, maxSentenceLength -> 1000, maxIter -> 1, stepSize -> 0.025)
  
  protected def wrapEmbeddingsMetadata(col: Column, vectorSize: Int): Column = {

    val metadataBuilder: MetadataBuilder = new MetadataBuilder()
    metadataBuilder.putString("annotatorType", AnnotatorType.WORD_EMBEDDINGS)
    metadataBuilder.putLong("dimension", vectorSize.toLong)
    
    col.as(col.toString, metadataBuilder.build)
    
  }

}
