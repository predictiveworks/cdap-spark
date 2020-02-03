package de.kp.works.text.chunk
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
import java.util.{List => JList}
import com.johnsnowlabs.nlp.annotators.pos.perceptron.PerceptronModel

import org.apache.spark.sql._
import de.kp.works.text.AnnotationBase

import scala.collection.JavaConversions._

class Chunker(model:PerceptronModel) extends AnnotationBase {
  
  def chunk(dataset:Dataset[Row], parsers:JList[String], textCol:String, tokenCol:String, chunkCol:String):Dataset[Row] = {
    
    /* Sample: ["(?:<JJ|DT>)(?:<NN|VBG>)+"] */
    val regexParsers = parsers.toArray.map(parser => parser.asInstanceOf[String])
    val document = normalizedTokens(dataset, textCol)
    
    model.setInputCols(Array("sentences", "token"))
    model.setOutputCol("tags")
    
    val tagged = model.transform(dataset)

    val chunker = new com.johnsnowlabs.nlp.annotators.Chunker()
    chunker.setInputCols(Array("sentences", "pos"))
    chunker.setOutputCol("chunk")

    chunker.setRegexParsers(regexParsers)
    val chunked = chunker.transform(tagged)
    
    val finisher = new com.johnsnowlabs.nlp.Finisher()
    .setInputCols(Array("token", "chunk"))
    .setOutputCols(tokenCol, chunkCol)

    finisher.transform(chunked)
    
  }
  
}