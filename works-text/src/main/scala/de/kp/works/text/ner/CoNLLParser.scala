package de.kp.works.text.ner
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
import org.apache.spark.ml.param.shared._

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.util._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

trait CoNLLParams extends Params {
   
  final val lineCol = new Param[String](CoNLLParams.this, "lineCol",
      "Name of the input text field that contains the annotated sentences for training purpose.", (value:String) => true)
  
  final val textCol = new Param[String](CoNLLParams.this, "textCol",
      "Name of the output text field that contains the extracted sentence.", (value:String) => true)
   
  final val documentCol = new Param[String](CoNLLParams.this, "documentCol",
      "Name of the output field that contains the text annotations of 'document' type.", (value:String) => true)
   
  final val sentenceCol = new Param[String](CoNLLParams.this, "sentenceCol",
      "Name of the output field that contains the sentence annotations of 'document' type.", (value:String) => true)
   
  final val tokenCol = new Param[String](CoNLLParams.this, "tokenCol",
      "Name of the output field that contains the token annotations of 'token' type.", (value:String) => true)
   
  final val posCol = new Param[String](CoNLLParams.this, "posCol",
      "Name of the output field that contains the part-of-speech annotations of 'pos' type.", (value:String) => true)
    
  final val labelCol = new Param[String](CoNLLParams.this, "labelCol",
      "Name of the output field that contains the named-entity annotations of 'named_entity' type.", (value:String) => true)

  def setLineCol(value:String): this.type = set(lineCol, value)
  setDefault(lineCol -> "line")
 
  def setTextCol(value:String): this.type = set(textCol, value)
  setDefault(textCol -> "text")
 
  def setDocumentCol(value:String): this.type = set(documentCol, value)
  setDefault(documentCol -> "document")
 
  def setSentenceCol(value:String): this.type = set(sentenceCol, value)
  setDefault(sentenceCol -> "sentence")
 
  def setTokenCol(value:String): this.type = set(tokenCol, value)
  setDefault(tokenCol -> "token")
 
  def setPosCol(value:String): this.type = set(posCol, value)
  setDefault(posCol -> "pos")
  
  def setLabelCol(value:String): this.type = set(labelCol, value)
  setDefault(labelCol -> "label")
  
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

class CoNLLParser(override val uid: String) extends Transformer with CoNLLParams {

  def this() = this(Identifiable.randomUID("coNLLParser"))

  def transform(dataset: Dataset[_]): DataFrame = {

    validateSchema(dataset.schema)
    /*
     * This method is a wrapper of the CoNLL case class from Spark-NLP;
     * We expect the dataset that specifies the CoNLL formatted training
     * data not to be too large 
     */
    val lines = dataset.select($(lineCol)).collect.map{case Row(line:String) => line}
    /*
     * The following parameters remain unchanged:
     * 
     * conllLabelIndex = 3,
     * conllPosIndex = 1,
     * explodeSentences = true
     * 
     */
    val coNLL = com.johnsnowlabs.nlp.training.CoNLL(
        documentCol = $(documentCol),
        tokenCol = $(tokenCol),
        posCol = $(posCol),
        conllTextCol = $(textCol),
        labelCol = $(labelCol))
    
    coNLL.readDatasetFromLines(lines, dataset.sparkSession).toDF

  }

  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  override def copy(extra: ParamMap): CoNLLParser = defaultCopy(extra)
  
}