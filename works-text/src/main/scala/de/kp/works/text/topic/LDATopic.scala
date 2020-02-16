package de.kp.works.text.topic
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

import org.apache.hadoop.fs.Path

import org.apache.spark.ml._
import org.apache.spark.ml.linalg.{Vector,Vectors}
import org.apache.spark.ml.clustering.{LDAModel, DistributedLDAModel}

import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._

import org.apache.spark.ml.util._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import de.kp.works.text.AnnotationBase
import scala.collection.mutable.{HashMap,WrappedArray}

case class TopicSpec(indices:Array[Int], weights:Array[Double])

trait LDATopicParams extends Params with AnnotationBase {
  
  final val textCol = new Param[String](this, "textCol",
      "Name of the input field that contains the text document.", (value:String) => true)
 
  def setTextCol(value:String): this.type = set(textCol, value)
  
  final val vocabSize = new Param[Int](this, "vocabSize",
      "The size of the vocabulary to build vector representations. Default is 10000.", (value:Int) => true)
 
  def setVocabSize(value:Int): this.type = set(vocabSize, value)

  /**
   * Param for the number of topics (clusters) to infer. Must be &gt; 1. Default: 10.
   */
  final val k = new IntParam(this, "k", "The number of topics (clusters) to infer. Must be > 1.", ParamValidators.gt(1))
 
  def setK(value:Int): this.type = set(k, value)

  /**
   * Param for maximum number of iterations (&gt;= 0).
   */
  final val maxIter: IntParam = new IntParam(this, "maxIter", "maximum number of iterations (>= 0)", ParamValidators.gtEq(0))
 
  def setMaxIter(value:Int): this.type = set(maxIter, value)

  final val maxTerms: IntParam = new IntParam(this, "maxTerms", "maximum number of terms to describe topic", ParamValidators.gt(0))
 
  def setMaxTerms(value:Int): this.type = set(maxTerms, value)

  setDefault(vocabSize -> 10000, maxIter -> 100, maxTerms -> 10)
  
  def tokensUDF = udf{annotations:WrappedArray[Row] => {
    
    val schema = annotations.head.schema
    val index = schema.fieldIndex("result")
    
    annotations.map(row => row.getString(index))
    
  }}
  
  def vectorizeUDF(vocab:Map[String,Int]) = udf{terms:WrappedArray[String] => {

    val counts = HashMap.empty[Int, Double]
    terms.foreach(term => {
      if (vocab.contains(term)) {      
        val idx = vocab(term)
        counts(idx) = counts.getOrElse(idx, 0.0) + 1.0
      }
    })
    
    Vectors.sparse(vocab.size, counts.toSeq)
    
  }}
  
  def validateSchema(schema:StructType):Unit = {
    
    /* TEXT FIELD */
    
    val textColName = $(textCol)  
    
    if (schema.fieldNames.contains(textColName) == false)
      throw new IllegalArgumentException(s"Text document column $textColName does not exist.")
    
    val textColType = schema(textColName).dataType
    if (!(textColType == StringType)) {
      throw new IllegalArgumentException(s"Data type of text document column $textColName must be StringType.")
    }
    
  }
  
}

class LDATopic(override val uid: String)
  extends Estimator[LDATopicModel] with LDATopicParams with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("ldaTopic"))
  
  override def fit(dataset: Dataset[_]): LDATopicModel = {
    
    /*
     * STEP #1: Normalize text document, build term vocabulary
     * and vectorize document terms based on the vocab.
     */
    var document = normalizedTokens(dataset.toDF, $(textCol))
    document = document.withColumn("terms", tokensUDF(col("token")))
    
    val rdd = document.rdd.map(row => {
      
      val schema = row.schema
      val index = schema.fieldIndex("terms")
      
      val terms = row.getAs[WrappedArray[String]](index)
      terms
      
    }).flatMap(items => items.map(item => (item,1))).reduceByKey(_ + _)
    
    val vocab = rdd.sortBy(- _._2).take($(vocabSize))
      .zipWithIndex.map{case((value,freq),index) => (value,index)}.toMap
      
    val vectorize = vectorizeUDF(vocab)
    document = document.withColumn("vector", vectorize(col("terms")))
    /*
     * STEP #2: Train LDA (distributed) model
     */
    val algo = new org.apache.spark.ml.clustering.LDA()

    algo.setK($(k))
    algo.setMaxIter($(maxIter))
    
    val optimizer = "em"
    algo.setOptimizer(optimizer)
    /*
     * The 'alpha' parameter of the Dirichlet prior 
     * on the per-document topic distributions;
     * 
     * it specifies the amount of topic smoothing to 
     * use (> 1.0) (-1 = auto)
     * 
     * the default value '-1' specifies the default 
     * symmetric document-topic prior
     */
    algo.setDocConcentration(-1)
    /*
     * The 'beta' parameter of the Dirichlet prior 
     * on the per-topic word distribution;
     * 
     * it specifies the amount of term (word) smoothing 
     * to use (> 1.0) (-1 = auto)
     * 
     * the default value '-1' specifies the default 
     * symmetric topic-word prior
     */      
    algo.setTopicConcentration(-1)
    
    algo.setFeaturesCol("vector")
    val model = algo.fit(document)
    
    copyValues(new LDATopicModel(uid, vocab, model).setParent(this))

  }

  override def transformSchema(schema:StructType):StructType = {
    schema
  }

  override def copy(extra:ParamMap):LDATopic = defaultCopy(extra)
  
}

class LDATopicModel(override val uid:String, vocab:Map[String,Int], model:LDAModel)
  extends Model[LDATopicModel] with LDATopicParams with MLWritable {

  import LDATopicModel._

  def this(vocab:Map[String,Int], model:LDAModel) = {
    this(Identifiable.randomUID("LDATopicModel"), vocab, model)
  }

  def getVocab:Map[String,Int] = vocab
  
  def getModel:LDAModel = model
  
  def describeTermsUDF(termsMap:Map[Int,String]) = udf{termIndices:WrappedArray[Int] => {
    termIndices.map{index => termsMap(index)}    
  }}
  
  def logPerplexity(dataset:Dataset[_]):Double = model.logPerplexity(dataset)

  def logLikelihood(dataset:Dataset[_]) = model.logLikelihood(dataset)

  def topics():Dataset[Row] = {

    /* The result desribes a dataframe with columns 'topic', 
     * 'termIndices' and 'termWeights'
     * 
     *  - "topic": IntegerType: topic index
     *  
   	 *  - "termIndices": ArrayType(IntegerType): term indices, 
   	 *  			sorted in order of decreasing term importance
   	 *  
   	 *  - "termWeights": ArrayType(DoubleType): corresponding 
   	 *  			sorted term weights
     */
    val spec = model.describeTopics($(maxTerms))
    
    val describeTerms = describeTermsUDF(vocab.map(_.swap))
    spec
      .withColumn("terms", describeTerms(col("termIndices")))
      .withColumnRenamed("termWeights", "weights")
      .drop("termIndices")

  }
  
  override def transform(dataset:Dataset[_]):DataFrame = {
    
    /*
     * STEP #1: Normalize text document, and vectorize 
     * document terms based on the vocab.
     */
    var document = normalizedTokens(dataset.toDF, $(textCol))
    document = document.withColumn("terms", tokensUDF(col("token")))
      
    val vectorize = vectorizeUDF(vocab)
    document = document.withColumn("vector", vectorize(col("terms")))
    
    val topics = model.transform(document)
    
    val finalize = udf{vector:Vector => {
      
      val indexed = vector.toArray.zipWithIndex
      
      val indices = indexed.map(_._2)
      val weights = indexed.map(_._1)
      
      TopicSpec(indices, weights)
      
    }}
    
    val output = model.transform(document)
      .withColumn("spec", finalize(col("topicDistribution")))
      .withColumn("topics",  col("spec").getItem("indices"))
      .withColumn("weights", col("spec").getItem("weights"))
      .drop("topicDistribution").drop("spec")
    
    output
    
  }

  override def transformSchema(schema:StructType):StructType = {
    schema
  }

  override def copy(extra:ParamMap):LDATopicModel = {
    val copied = new LDATopicModel(uid, vocab, model).setParent(parent)
    copyValues(copied, extra)
  }

  override def write: MLWriter = new LDATopicModelWriter(this)

}

object LDATopicModel extends MLReadable[LDATopicModel] {

  private case class Data(word: String, index: Int)
  
  class LDATopicModelWriter(instance: LDATopicModel) extends MLWriter {

    override def save(path:String): Unit = {
      super.save(path)
    }

    override def saveImpl(path: String): Unit = {
      
      /* Save metadata & params */
      SparkParamsWriter.saveMetadata(instance, path, sc)      
      
      /* Save vocabulary */
      val vocabPath = new Path(path, "vocab").toString

      val vocab = instance.getVocab
      val spark = sparkSession
      import spark.implicits._
      spark.createDataset[(String, Int)](vocab.toSeq)
        .map { case (word, index) => Data(word, index) }
        .toDF
        .write
        .parquet(vocabPath)
      
      /* Save model */
      val modelPath = new Path(path, "model").toString
      
      val model = instance.getModel
      model.save(modelPath)
      
    }
    
  }

  private class LDATopicModelReader extends MLReader[LDATopicModel] {

    private val className = classOf[LDATopicModel].getName

    override def load(path: String):LDATopicModel = {
      
      /* Read metadata & params */
      val metadata = SparkParamsReader.loadMetadata(path, sc, className)

      /* Read vocabulary */
      val sparkSession = super.sparkSession
      import sparkSession.implicits._

      val vocabPath = new Path(path, "vocab").toString
      val vocab =  sparkSession.read.parquet(vocabPath).as[Data]
        .map{data => (data.word, data.index)}
        .collect
        .toMap
      
      /* Read LDA model */
      val modelPath = new Path(path, "model").toString
      val ldaModel = DistributedLDAModel.load(modelPath)
     
      /*
       * Reconstruct trained model instance
       */
      val model = new LDATopicModel(metadata.uid, vocab, ldaModel)
      SparkParamsReader.getAndSetParams(model, metadata)
      
      model

    }
  }

  override def read: MLReader[LDATopicModel] = new LDATopicModelReader

  override def load(path: String): LDATopicModel = super.load(path)
  
}