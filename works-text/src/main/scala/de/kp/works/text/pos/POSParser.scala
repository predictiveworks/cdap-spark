package de.kp.works.text.pos
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

import com.johnsnowlabs.nlp.{Annotation, AnnotatorType}

import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.util._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

/** INTERNAL POS TAGGING MODEL **/

private case class TaggedToken(
    token: String, 
    tag: String)
    
private case class Annotations(
    text: String, 
    document: Array[Annotation], 
    pos: Array[Annotation])

trait POSParams extends Params {
   
  final val lineCol = new Param[String](POSParams.this, "lineCol",
      "Name of the input text field that contains the annotated sentences for training purpose.", (value:String) => true)
  
  final val textCol = new Param[String](POSParams.this, "textCol",
      "Name of the output text field that contains the extracted sentence.", (value:String) => true)
   
  final val documentCol = new Param[String](POSParams.this, "documentCol",
      "Name of the output field that contains the annotations of 'document' type.", (value:String) => true)
   
  final val posCol = new Param[String](POSParams.this, "posCol",
      "Name of the output field that contains the annotations of 'pos' type.", (value:String) => true)
   
  final val delimiter = new Param[String](POSParams.this, "delimiter",
      "The delimiter in the input text line to separate tokens and POS tags.", (value:String) => true)
 
  def setLineCol(value:String): this.type = set(lineCol, value)
  setDefault(lineCol -> "line")
 
  def setTextCol(value:String): this.type = set(textCol, value)
  setDefault(textCol -> "text")
 
  def setDocumentCol(value:String): this.type = set(documentCol, value)
  setDefault(documentCol -> "document")
 
  def setPosCol(value:String): this.type = set(posCol, value)
  setDefault(posCol -> "pos")
 
  def setDelimiter(value:String): this.type = set(delimiter, value)
  setDefault(delimiter -> "|")
 
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
/**
 * This class transforms a dataset of lines into a dataset that is prepared to train a POS model.
 * The provided corpus must be organized as sentence-per-line with the following format:
 * 
 * To|TO help|VB you|PRP see|VB how|WRB much|JJ your|PRP$ contribution|NN means|VBZ ,|, I|PRP 'm|VBP sharing|VBG with|IN you|PRP The|DT words|NNS of|IN people|NNS who|WP have|VBP lived|VBN Goodwill|NNP 's|POS mission|NN .|. 
 * 
 * We|PRP want|VBP you|PRP to|TO Know|VBP why|WRB your|PRP$ support|NN of|IN Goodwill|NNP is|VBZ so|RB important|JJ .|.
 */

class POSParser(override val uid: String) extends Transformer with POSParams {

  def this() = this(Identifiable.randomUID("posParser"))

  def transform(dataset: Dataset[_]): DataFrame = {

    validateSchema(dataset.schema)    
    require($(delimiter).length == 1, s"Delimiter must be one character long.")
    
    val line = $(lineCol)
    val line2document = line2annotations_udf($(delimiter))

    val text     = $(textCol)
    val document = $(documentCol)
    val pos      = $(posCol)
    
    val trainset = dataset
      /* 
       * Reduce dataset to a single text line and remove
       * lines with empty content
       */
      .select(line)
      .filter(!(col(line) === ""))
      .withColumn("_annotations", line2document(col(line))).drop(line)
      /*
       * Unpack derived annotations instance into columns
       * 
       * - textCol
       * - documentCol
       * - posCol
       */
      .withColumn(text, col("_annotations").getItem("text"))
      .withColumn("document", col("_annotations").getItem("document"))
      .withColumn("pos", col("_annotations").getItem("pos"))
      .drop("_annotations")
      .withColumn(document, 
          wrapColumnMetadata(col("document"), AnnotatorType.DOCUMENT, document))
      .withColumn(pos,
          wrapColumnMetadata(col("pos"), AnnotatorType.POS, pos))
          
    trainset.select(text, document, pos)

  }  
  /*
   * A helper method to extract the sentence, tokens and tags from
   * each input line and transform into an Annotations instance
   */
  private def line2annotations_udf(delimiter:String) = udf((line: String) => {

    /**** INLINE HELPER ****/

    def createDocumentAnnotation(sentence: String) = {
      Array(Annotation(
        AnnotatorType.DOCUMENT,
        0,
        sentence.length - 1,
        sentence,
        Map.empty[String, String]
      ))
    }

    def createPosAnnotation(sentence: String, taggedTokens: Array[TaggedToken]) = {
      var lastBegin = 0
      taggedTokens.map { case TaggedToken(token, tag) =>
        val tokenBegin = sentence.indexOf(token, lastBegin)
        val a = Annotation(
          AnnotatorType.POS,
          tokenBegin,
          tokenBegin + token.length - 1,
          tag,
          Map("word" -> token)
        )
        lastBegin += token.length
        a
      }
    }
    
    /**** TRANSFORMATION ****/
    
    val splitted = line.split(" ").map(_.trim)

    val tokenTags = splitted.flatMap(token => {

      val tokenTag = token.split(delimiter.head).map(_.trim)
      if (tokenTag.exists(_.isEmpty) || tokenTag.length != 2)
        // Ignore broken pairs or pairs with delimiter char
        None
      
      else
        Some(TaggedToken(tokenTag.head, tokenTag.last))
    
    })

    val sentence = tokenTags.map(_.token).mkString(" ")
 
    val documentAnnotation = createDocumentAnnotation(sentence)
    val posAnnotation = createPosAnnotation(sentence, tokenTags)
    
    Annotations(sentence, documentAnnotation, posAnnotation)
    
  })
  
  /*
  * Add Metadata annotationType to output DataFrame
  * NOTE: This should be replaced by an existing function when it's accessible in next release
  * */

  private def wrapColumnMetadata(col: Column, annotatorType: String, outPutColName: String): Column = {
    val metadataBuilder: MetadataBuilder = new MetadataBuilder()
    metadataBuilder.putString("annotatorType", annotatorType)
    col.as(outPutColName, metadataBuilder.build)
  }

  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  override def copy(extra: ParamMap): POSParser = defaultCopy(extra)

}
