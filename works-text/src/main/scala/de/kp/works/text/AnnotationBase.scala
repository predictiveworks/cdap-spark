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
import org.apache.spark.sql._

trait AnnotationBase {
  
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
  
  def tokenizedSentences(dataset:Dataset[Row], textCol:String):Dataset[Row] = {
    
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
  def normalizedTokens(dataset:Dataset[Row], textCol:String):Dataset[Row] = {
    
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
    /*
     * Tokenizer will separate each meaningful word, within each 
     * sentence. This is very important on NLP since it will identify 
     * a token boundary.
     */
    val tokenizer = new nlp.annotators.Tokenizer()
    tokenizer.setInputCols("sentences")
    tokenizer.setOutputCol("token")
    
    document = tokenizer.fit(document).transform(document)
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
    
    document = normalizer.fit(document).transform(document)
    document
    
  }
}