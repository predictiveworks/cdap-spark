package de.kp.works.text
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

import com.johnsnowlabs.nlp

import org.apache.spark.ml.linalg.Vectors

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.collection.mutable.WrappedArray

case class DateMatcherResult(sentence:String, odate:String, ndate:String)

trait AnnotationBase {
  /*
   * A helper method to finalize the result of the date matcher
   * annotation stage; two different options are supported: "extract"
   * and "replace"
   */
  def finishDateMatcher(option:String) = udf{(sentences:WrappedArray[Row], dates:WrappedArray[Row]) => {
      /*
       * The extracted number of dates may not match the 
       * number of sentences; therefore the sentences are
       * transformed into an index dictionary to enable
       * proper assignmed of extracted dates
       */
      val sidx = sentences.head.schema.fieldIndex("result")
      val indexed = sentences.map(_.getString(sidx)).zipWithIndex
      /*
       * Extract date annotations and thereby assign the
       * reference to the respective sentence
       */
      val bidx = dates.head.schema.fieldIndex("begin")
      val eidx = dates.head.schema.fieldIndex("end")
      
      val didx = dates.head.schema.fieldIndex("result")
      val midx = dates.head.schema.fieldIndex("metadata")
      
      val dateMap = dates.map(row => {

        val date = row.getString(didx)
        /* 
         * Start and end indices to retrieve the original
         * date or time expression from the respective 
         * sentence
         */
        val begin = row.getInt(bidx)
        val end   = row.getInt(eidx) + 1
        
        val index = row.getJavaMap[String,String](midx).get("sentence")
        
        (index.toInt, (begin, end, date))
        
      }).toMap
      
      /* 
       * Finally finish result with respect to the provided
       * option parameter 
       */
      val result = indexed.map{case(sentence,index) => {
        
        option match {
          case "extract" => {
        
            if (dateMap.contains(index)) {
              
              val (begin, end, date) = dateMap(index)
              val odate = sentence.substring(begin, end)
 
              Array(sentence, odate, date)
              
            } else {
              /* 
               * The extracted sentences does not contain any data 
               * or time expression; initial & extracted date is set
               * to an empty String
               */
              Array(sentence, "", "")

            }
            
          }
          case "replace" => {
        
            if (dateMap.contains(index)) {
              
              val (begin, end, date) = dateMap(index)
              val odate = sentence.substring(begin, end)

              val replaced = sentence.replace(odate, date)
              Array(replaced)
              
            } else {
              /* 
               * The extracted sentences does not contain any data
               * or time expression; the sentence is returned without
               * any change
               */
             Array(sentence)
            }
                        
          }
          case _ => throw new IllegalArgumentException(s"Option parameter '$option' is not supported")
        }
        
      }}
      
      result
      
    
  }}
  /*
   * A helper method to finalize the result of the POS tagging approach;
   * it concatenates tokens and tags and returns a single set of mixed
   * tokens separatetd by whitespaces
   */
  def finishPOSTagger = udf{(tokens:WrappedArray[Row], pos:WrappedArray[Row]) => {
   
    val tSchema = tokens.head.schema
    val tridx = tSchema.fieldIndex("result")
   
    val pSchema = pos.head.schema
    val pridx = pSchema.fieldIndex("result")

    val mixin = tokens.zip(pos).map{case(token, tag) => {
      s"${token.getString(tridx)}__${tag.getString(pridx)}"
    }}

    mixin.mkString(" ")
    
  }}
  
  /*
   * A helper method to finalize the result of the Norvig spell checking
   * approach; as this mechanism is assigned to noise reduction, the plugin
   * returns the corrected sentence
   */
  def finishSpellChecker(threshold:Double) = udf{(tokens:WrappedArray[Row], suggestions:WrappedArray[Row]) => {
      
    val tSchema = tokens.head.schema
    val tridx = tSchema.fieldIndex("result")
    
    val sSchema = suggestions.head.schema
    val sridx = sSchema.fieldIndex("result")
    val smidx = sSchema.fieldIndex("metadata")
    
    val corrections = tokens.zip(suggestions).map{case(token, suggestion) => {

      val term = token.getString(tridx)
      
      val confidence = suggestion.getJavaMap[String,String](smidx).get("confidence").toDouble
      val spelling = suggestion.getString(sridx)
      
      if (term != spelling && confidence > threshold) 
        spelling

      else term
      
    }}

    corrections.mkString(" ")

  }}
  /**
   * This is a helper method that concatenates the result of a 
   * tokenization stage into a whitespace separated sequence of 
   * tokens. The output can be used as 'text document' with other
   * text analysis plugins.
   */
  def finishTokens = udf{tokens:WrappedArray[Row] => {
    
    val schema = tokens.head.schema
    val ridx = schema.fieldIndex("result")
    
    tokens.map(row => row.getString(ridx)).mkString(" ")
    
  }}
  
  def detectedSentences(dataset:Dataset[Row], textCol:String):Dataset[Row] = {
    
    var document = dataset
    /*
     * DocumentAssembler is the annotator taking the target text column, 
     * making it ready for further NLP annotations, into a column called 
     * document.
     */
		val assembler = new nlp.DocumentAssembler()
		assembler.setInputCol(textCol)
		assembler.setOutputCol("document")
		
		document = assembler.transform(document);
    /*
     * SentenceDetector will identify sentence boundaries in paragraphs. 
     * Since we are reading entire file contents on each row, we want to 
     * make sure our sentences are properly bounded. 
     * 
     * This annotators correctly handles page breaks, paragraph breaks, 
     * lists, enumerations, and other common formatting features that 
     * distort the regular flow of the text. 
     * 
     * This help the accuracy of the rest of the pipeline. The output 
     * column for this is the sentence column.
     */
		val detector = new nlp.annotators.sbd.pragmatic.SentenceDetector()
    detector.setInputCols("document")
    detector.setOutputCol("sentences")   
    
    document = detector.transform(document)
    document
    
  }
  
  def extractTokens(dataset:Dataset[Row], textCol:String, normalization:Boolean):Dataset[Row] = {
       
      if (normalization == true)
        normalizedTokens(dataset, textCol)

      else
        tokenizedSentences(dataset, textCol)
 
  }
  
  def tokenizedSentences(dataset:Dataset[Row], textCol:String):Dataset[Row] = {
    
    var document = detectedSentences(dataset, textCol)
    /*
     * Tokenizer will separate each meaningful word, within each 
     * sentence. This is very important on NLP since it will identify 
     * a token boundary.
     */
    val tokenizer = new nlp.annotators.Tokenizer()
    tokenizer.setInputCols("sentences")
    tokenizer.setOutputCol("token")
    
    document = tokenizer.fit(document).transform(document)
    document
    
  }
  
  /**
   * A helper method to annotate a raw text document up to normalized tokens
   */
  def normalizedTokens(dataset:Dataset[Row], textCol:String, cleanupRegex:String = "[^A-Za-z0-9-]"):Dataset[Row] = {
    
    var document = tokenizedSentences(dataset, textCol)
    /*
		 * Normalizer will clean up each token, taking as input column token out 
		 * from our Tokenizer, and putting normalized tokens in the normal column. 
		 * 
		 * Cleaning up includes removing any non-character strings. 
		 * 
		 * Whether this helps or not on a model, is a decision of the model maker.    
		 */
    val normalizer = new nlp.annotators.Normalizer()
    normalizer.setInputCols("token")
    normalizer.setOutputCol("token")
    normalizer.setCleanupPatterns(Array(cleanupRegex))
    
    document = normalizer.fit(document).transform(document)
    document
    
  }

  /*
   * This helper method aggregates the word embeddings computed
   * through the provided Word2Vec embedding model and leverages
   * a pooling strategy (average | sum) to retrieve a feature
   * vector from all token embeddings    
   */
  def embeddings2vector_udf(strategy:String) = udf{annotations:WrappedArray[Row] => {
      
    val schema = annotations.head.schema
    val index = schema.fieldIndex("embeddings")

    val matrix = annotations.map(row => row.getAs[WrappedArray[Float]](schema.fieldIndex("embeddings")))

    val res = Array.ofDim[Float](matrix(0).length)
    matrix(0).indices.foreach {
      j =>
        matrix.indices.foreach {
          i =>
            res(j) += matrix(i)(j)
        }
        if (strategy == "AVERAGE")
          res(j) /= matrix.length
    }

    Vectors.dense(res.map(_.toDouble))
      
  }}
  
}