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

trait TreeBankParams extends Params {
   
  final val lineCol = new Param[String](TreeBankParams.this, "lineCol",
      "Name of the input text field that contains the annotated sentences for training purpose.", (value:String) => true)
  
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

class TreeBankParser(override val uid: String) extends Transformer with TreeBankParams {

  def this() = this(Identifiable.randomUID("treeBankParser"))

  def transform(dataset: Dataset[_]): DataFrame = {

    validateSchema(dataset.schema)

    val lines = dataset.select($(lineCol)).collect.map{case Row(line:String) => line}
    /*
     * A sentence is spread about multiple lines and each sentence is
     * seperated by an empty line
     */
    val buffer = StringBuilder.newBuilder
    lines.foreach(line => {
      if (line.isEmpty) buffer.append("##") else buffer.append(line + "#")
    })

    val text = buffer.toString()
    val sections = text.split("##").toList

    val sentences = sections.map(
      s => {
        val lines = s.split("#").toList
        val body  = lines.map( l => {
          val arr = l.split("\\s+")
          val (raw, pos, dep) = (arr(0), arr(1), arr(2).toInt)
          // CONLL dependency layout assumes [root, word1, word2, ..., wordn]  (where n == lines.length)
          // our   dependency layout assumes [word0, word1, ..., word(n-1)] { root }
          val dep_ex = if(dep==0) lines.length+1-1 else dep-1
          WordData(raw, pos, dep_ex)
        })
        body  // Don't pretty up the sentence itself
      }
    ).map(words => CoNLLUSentence(words))

    import dataset.sparkSession.implicits._
    sentences.toDF("sentence")

  }

  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  override def copy(extra: ParamMap): TreeBankParser = defaultCopy(extra)
  
}