package de.kp.works.text.associations
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

import org.apache.spark.ml.fpm.FPGrowth

import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.util._

import org.apache.spark.sql._

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.mutable.WrappedArray

case class NERelation(
    ante_tok:String, 
    ante_tag:String, 
    cons_tok:String,
    cons_tag:String,
    token_score:Double,
    document_score:Double)

trait RelNERParams extends Params {
    
  final val tokenCol = new Param[String](this, "tokenCol",
      "Name of the input column that contains the list of annotated tokens.", (value:String) => true)
   
  final val nerCol = new Param[String](this, "nerCol",
      "Name of the input column that contains the list of NER annotation for the provided tokens.", (value:String) => true)

  /**
   * Minimal support level of the frequent pattern. [0.0, 1.0]. Any pattern that appears
   * more than (minSupport * size-of-the-dataset) times will be output in the frequent itemsets.
   * Default: 0.3
   */
  val minSupport: DoubleParam = new DoubleParam(this, "minSupport", 
      "The minimal support level of a frequent pattern", ParamValidators.inRange(0.0, 1.0))

  def getMinSupport: Double = $(minSupport)
 
  def setTokenCol(value:String): this.type = set(tokenCol, value)
 
  def setNerCol(value:String): this.type = set(nerCol, value)

  def setMinSupport(value: Double): this.type = set(minSupport, value)
  
  setDefault(tokenCol -> "token", nerCol -> "ner", minSupport -> 0.0001)

  def validateSchema(schema:StructType):Unit = {
    
    /* TOKEN FIELD */
    
    val tokenColName = $(tokenCol)  
    
    if (schema.fieldNames.contains(tokenColName) == false)
      throw new IllegalArgumentException(s"Token column $tokenColName does not exist.")
    
    val tokenColType = schema(tokenColName).dataType
    if (!(tokenColType == ArrayType(StringType))) {
      throw new IllegalArgumentException(s"Data type of token column $tokenColName must be ArrayType(StringType).")
    }
    
    /* NER FIELD */
    
    val nerColName = $(nerCol)  
    
    if (schema.fieldNames.contains(nerColName) == false)
      throw new IllegalArgumentException(s"NER annotation column $nerColName does not exist.")
    
    val nerColType = schema(nerColName).dataType
    if (!(nerColType == ArrayType(StringType))) {
      throw new IllegalArgumentException(s"Data type of NER annotation column $tokenColName must be ArrayType(StringType).")
    }
    
  }
}
/**
 * This transformer executes a dataset that has been pre-processed by
 * an NER Predictor and leverages the token and ner column to build
 * association rules from the respective recognized entities.
 * 
 * This transformer is a pre-processing stage for building graphs
 * of named entities for a certain text document
 */
class RelNER(override val uid: String) extends Transformer with RelNERParams {

  def this() = this(Identifiable.randomUID("relationNER"))
 
  def entities_udf(removables:Array[String]) = udf((tokens:WrappedArray[String], tags:WrappedArray[String]) => {
      
    val per = Array("B-PER","I-PER")
    def isPer(tag:String) = per.contains(tag)

    val org = Array("B-ORG","I-ORG")
    def isOrg(tag:String) = org.contains(tag)

    val loc = Array("B-LOC","I-LOC")
    def isLoc(tag:String) = loc.contains(tag)

    val zipped = tokens.zip(tags)
    /*
     * Check whether two tokens that directly follow each other 
     * have the same tag and restrict to those that do not match 
     * the list of filter tags
     */
    val filtered = (zipped.zip(zipped.tail).map(pairs => {
        
      val ante = pairs._1
      val cons = pairs._2
        
      if (isPer(ante._2) && isPer(cons._2))
        (ante._1 + " " + cons._1, "I-PER")
        
      else if (isOrg(ante._2) && isOrg(cons._2))
        (ante._1 + " " + cons._1, "I-ORG")
        
      else if (isLoc(ante._2) && isLoc(cons._2))
        (ante._1 + " " + cons._1, "I-LOC")
        
      else (ante._1, ante._2)
        
    }) ++ Array(zipped.last)).filter(p => removables.contains(p._2) == false)
    /*
     * STEP #2: Merge tokens and associated tags and restricted
     * to those that match the list of filter tags
     */
    if (filtered.isEmpty) null
    else {
      /*
       * This transformer leverages association rule mining and
       * this algorithm requires a white space separated list of
       * Strings
       */
      val transaction = filtered.map(p => p._1 + "#" + p._2).distinct
      transaction
   
    }    
  })
  
  def transform(dataset: Dataset[_]): DataFrame = {

    /*
     * Exclude all tokens that are not tagged as named entities
     * and transform tokens and their tags into a whitespace
     * separated list as input for Frequent Pattern Mining
     */
    val entities = entities_udf(Array("O"))
    val transactions = dataset.withColumn("items", entities(col($(tokenCol)), col($(nerCol)))).filter(col("items").isNotNull)
    /*
     * Build Frequent Pattern Mining model leveraging
     * FPGrowth algorithm
     */
    val fpGrowth = new FPGrowth().setItemsCol("items")
      .setMinSupport($(minSupport))

    val model = fpGrowth.fit(transactions)
    val itemsets = model.freqItemsets
    
    /*
     * Restrict to those named entities that appear together (co-occurrence)
     */
    val sizeUDF = udf{items:WrappedArray[String] => items.size}
    val cooccurrences = itemsets.withColumn("size", sizeUDF(col("items"))).filter(col("size") === 2)
    /*
     * This evaluation stage score named entities that co-occur:
     * 
     * Person, Organization, Location, Time and Misc
     * 
     * - B-PER, describes the first word in a named entity of type PER
     * - I-PER
     * 
     * - B-ORG, describes the first word in a named entity of type ORG
     * - I-ORG
     * 
     * - B-LOC, describes the first word in a named entity of type LOC
     * - I-LOC
     * 
     * - I-TIME
     * 
     * - I-MISC
     * 
     */
    val scoreUDF = udf{(items:WrappedArray[String], freq:Int) => {
      
      val per = Array("B-PER","I-PER")
      def isPer(tag:String) = per.contains(tag)

      val org = Array("B-ORG","I-ORG")
      def isOrg(tag:String) = org.contains(tag)

      val loc = Array("B-LOC","I-LOC")
      def isLoc(tag:String) = loc.contains(tag)
      
      val msc = Array("I-TIME", "I-MISC")
      def isMsc(tag:String) = msc.contains(tag)
      
      
      val Array(tok1,tag1) = items(0).split("#")
      val Array(tok2,tag2) = items(1).split("#")
      /*
       * Scoring
       */
      def contains(tag1:String, tag2:String) = {
        (tag1.contains(tag2) || tag2.contains(tag1))
      }
      
      val token_score =
        /*** 1.0 ***/
        if (isPer(tag1) && isPer(tag2)) {
          if (contains(tag1,tag2)) 0.0 else 1.0
        } 
        else if (isOrg(tag1) && isOrg(tag2)) {
          if (contains(tag1,tag2)) 0.0 else 1.0
        }
        else if (isLoc(tag1) && isLoc(tag2)) {
          if (contains(tag1,tag2)) 0.0 else 1.0
        }
        /*** 0.5 ***/
        else if (isPer(tag1) && (isOrg(tag2) || isLoc(tag2))) {
          0.5
        }
        else if (isPer(tag2) && (isOrg(tag1) || isLoc(tag1))) {
          0.5
        }
        else if (isOrg(tag1) && (isPer(tag2) || isLoc(tag2))) {
          0.5
        }
        else if (isOrg(tag2) && (isPer(tag1) || isLoc(tag1))) {
          0.5
        }
        else if (isLoc(tag1) && (isPer(tag2) || isOrg(tag2))) {
          0.5
        }
        else if (isLoc(tag2) && (isPer(tag1) || isOrg(tag1))) {
          0.5
        }
        /*** 0.25 ***/
         else if (isMsc(tag1) || isMsc(tag2)) {
          0.25

         } else
          0.0
      
      val document_score = token_score * freq
      
      NERelation(
          ante_tok=tok1,
          cons_tok=tok2,
          ante_tag=tag1,
          cons_tag=tag2,
          token_score = token_score,
          document_score = document_score)
     
    }}

    cooccurrences
      .withColumn("rel", scoreUDF(col("items"),col("freq")))
      .withColumn("ante_token", col("rel").getItem("ante_tok"))
      .withColumn("ante_tag",   col("rel").getItem("ante_tag"))
      .withColumn("cons_tok",   col("rel").getItem("cons_tok"))
      .withColumn("cons_tag",   col("rel").getItem("cons_tag"))
      .withColumn("tok_score",  col("rel").getItem("token_score"))
      .withColumn("doc_score",  col("rel").getItem("document_score"))
      .filter(col("tok_score") > 0.0)
      .drop("items").drop("freq").drop("size").drop("rel")

  }

  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  override def copy(extra: ParamMap): RelNER = defaultCopy(extra)
 
}