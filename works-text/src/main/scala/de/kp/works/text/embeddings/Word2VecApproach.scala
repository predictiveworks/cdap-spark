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

import com.johnsnowlabs.nlp.annotators.common.SentenceSplit
import com.johnsnowlabs.nlp._
import com.johnsnowlabs.nlp.AnnotatorType.{DOCUMENT, TOKEN, WORD_EMBEDDINGS}

import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.feature.{Word2Vec => SparkWord2Vec}
import org.apache.spark.ml.linalg.Vector

import org.apache.spark.ml.param._
import org.apache.spark.ml.util._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.mutable.WrappedArray
/**
 * [Word2VecApproach] is a seamless integration of Apache Spark [Word2Vec]
 * and Spark NLP Annotator model to support the use Word2Vec vectorization
 * with the annotation mechanism of Spark NLP
 */
class Word2VecApproach(override val uid: String) extends AnnotatorApproach[Word2VecModel] with Word2VecParams {

  def this() = this(Identifiable.randomUID("WORD_EMBEDDINGS"))

  override val outputAnnotatorType: AnnotatorType = WORD_EMBEDDINGS

  /** Annotator reference id. Used to identify elements in metadata or to refer to this annotator type */
  override val inputAnnotatorTypes: Array[String] = Array(DOCUMENT, TOKEN)

  override val description: String = "Word2Vec embeddings lookup annotator that maps tokens to vectors"

  override def beforeTraining(sparkSession: SparkSession): Unit = {
    /* no-ops */
  }

  override def train(dataset: Dataset[_], recursivePipeline: Option[PipelineModel]): Word2VecModel = {

    /*
	   * STEP #1: Evaluates the input cols and transform tokens 
	   * into an Array[String] as annotated tokens are usually 
	   * normalized and of better quality than raw terms
	   */    
    val cols = getInputCols.map(c => dataset.col(c))
    val tokens_udf = udf{(documents:WrappedArray[Row], tokens:WrappedArray[Row]) => {
      /*
       * Check which column contains the normalized terms
       */
      val document = documents.head
      val documentType = document.getString(document.schema.fieldIndex("annotatorType"))
      
      val terms = if (documentType == AnnotatorType.TOKEN) {
        documents.map(row => row.getString(row.schema.fieldIndex("result")))
      
      } else {
        tokens.map(row => row.getString(row.schema.fieldIndex("result")))
      }

      terms
      
    }}
    
    val trainset = dataset.withColumn("_word2vec", tokens_udf(cols: _*))
    /*
     * STEP #2: Train Apache Spark Word2VecModel and
     * extract the respective vocabulary
     */
    val words2vec = new SparkWord2Vec()
    words2vec.setMaxIter($(maxIter))
    words2vec.setStepSize($(stepSize))
    
    words2vec.setVectorSize($(vectorSize))
    words2vec.setWindowSize($(windowSize))
    
    words2vec.setMinCount($(minCount))
    words2vec.setMaxSentenceLength($(maxSentenceLength))
    
    words2vec.setInputCol("_word2vec")
    /*
     * STEP #3: Return vectors as dataframe with two fields, 
     * "word" and "vector", with "word" being a String and 
     * "vector" the Vector that it is mapped to.
     */
    val vector2Array = udf{vector:Vector => vector.toArray.map(_.toFloat)}
    val vectors = words2vec.fit(trainset).getVectors
    
    /* Build vocabulary to use in Word2VecModel */
    val vocab = vectors.collect
      .map{case Row(term:String, vector:Vector) => (term, vector.toArray.map(_.toFloat))}.toMap
    
    /* STEP #4: Initialize Word2VecModel */
    
    val model = new Word2VecModel(vocab).setOutputCol($(embeddingsCol))
    model
    
  }

}

object Word2VecApproach extends DefaultParamsReadable[Word2VecApproach]