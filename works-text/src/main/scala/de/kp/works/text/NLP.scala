package de.kp.works.text
/*
 * Copyright (c) 2019 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
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
import org.apache.spark.sql.functions._

import java.util.{Properties, List => JList}
import scala.collection.mutable

object NLP extends AnnotationBase {

  def assembleDocument(dataset:Dataset[Row], textCol:String, documentCol:String):Dataset[Row] = {

    var document = dataset
    /*
     * DocumentAssembler is the annotator taking the
     * target text column,  making it ready for further
     * NLP annotations, into a column called document.
     */
    val assembler = new nlp.DocumentAssembler()
    assembler.setInputCol(textCol)
    assembler.setOutputCol(documentCol)

    assembler.transform(document)

  }

  def detectSentence(dataset:Dataset[Row], textCol:String, sentenceCol:String):Dataset[Row] = {

    val detected = detectedSentences(dataset, textCol)

    val finisher = new nlp.Finisher()
    finisher.setInputCols("sentences")
    finisher.setOutputCols(sentenceCol)

    finisher.transform(detected)

  }

  def cleanStopwords(dataset:Dataset[Row], stopWords: JList[String], textCol:String, cleanedCol:String):Dataset[Row] = {

    val words = stopWords.toArray.map(word => word.asInstanceOf[String])
    var document = normalizedTokens(dataset, textCol)

    val cleaner = new nlp.annotators.StopWordsCleaner()

    cleaner.setInputCols("token")
    cleaner.setOutputCol("cleaned")

    cleaner.setStopWords(words)
    cleaner.setCaseSensitive(false)

    document = cleaner.transform(document)

    val dropCols = Array("document", "sentences", "token", "cleaned")
    document
      .withColumn(cleanedCol, finishTokens(col("cleaned")))
      .drop(dropCols: _*)

  }

  def generateNgrams(dataset:Dataset[Row], n:Int, textCol:String, ngramCol:String):Dataset[Row] = {

    val normalized = normalizedTokens(dataset, textCol)

    val generator = new com.johnsnowlabs.nlp.annotators.NGramGenerator()
    generator.setN(n)

    generator.setInputCols("token")
    generator.setOutputCol("ngram")

    val ngrams = generator.transform(normalized)

    val finisher = new com.johnsnowlabs.nlp.Finisher()
    finisher.setInputCols("ngram")
    finisher.setOutputCols(ngramCol)

    finisher.transform(ngrams)

  }

  def stemToken(dataset:Dataset[Row], textCol:String, stemCol:String):Dataset[Row] = {

    var document = normalizedTokens(dataset, textCol)
    /*
     * Stemmer will hard-stems out of words with the objective of retrieving
     * the meaningful part of the word.
     */
    val stemmer = new nlp.annotators.Stemmer()
    stemmer.setInputCols("token")
    stemmer.setOutputCol("stem")

    document = stemmer.transform(document)

    val dropCols = Array("document", "sentences", "token", "stem")
    document
      .withColumn(stemCol, finishTokens(col("stem")))
      .drop(dropCols: _*)

  }

  def tokenizeSentences(dataset:Dataset[Row], textCol:String, tokenCol:String):Dataset[Row] = {

    val tokenized = tokenizedSentences(dataset, textCol)

    val finisher = new nlp.Finisher()
    finisher.setInputCols("token")
    finisher.setOutputCols(tokenCol)

    finisher.transform(tokenized)

  }

  def normalizeToken(dataset:Dataset[Row], textCol:String, tokenCol:String):Dataset[Row] = {

    val normalized = normalizedTokens(dataset, textCol)

    val finisher = new nlp.Finisher()
    finisher.setInputCols("token")
    finisher.setOutputCols(tokenCol)

    finisher.transform(normalized)

  }

  def matchDate(dataset:Dataset[Row], props:Properties):Dataset[Row] = {
    /*
     * The Spark NLP matcher extracts only ONE date per sentence. Therefore, the
     * provided text document must be organized in sentences before data matching
     * is applied
     */
    val inputCol = props.getProperty("input.col")
    val outputCol = props.getProperty("output.col")

    val dateFormat = props.getProperty("date.format")
    val outputOption = props.getProperty("output.option")

    /** SENTENCES **/

    var document = detectedSentences(dataset, inputCol)

    /** MATCHER **/

    val matcher = new nlp.annotators.DateMatcher()
    matcher.setFormat(dateFormat)

    matcher.setInputCols("sentences")
    matcher.setOutputCol("date")

    document = matcher.transform(document)

    /** FINISHER **/

    val finalize = finishDateMatcher(outputOption)
    document = document.withColumn("res", finalize(col("sentences"), col("date")))
    /*
     * The representation of the output depends
     * on the provided output option; the finalize
     * stage returns an Array[Array[String]]
     */
    val dropCols = Array("document", "sentences", "date", "res")
    outputOption match {
      case "extract" =>

        val concat = udf{res:mutable.WrappedArray[mutable.WrappedArray[String]] => {
          res.map(item => {
            val Array(_, odate, ndate) = item.toArray
            odate + "=" + ndate
          }).filter(item => item != "=")
        }}

        document.withColumn(outputCol, concat(col("res"))).drop(dropCols: _*)
      case "replace" =>
        /*
         * This option returns a list of extracted sentences
         * with the detected date or time expression replaced
         * by its unified format
         */
        document.withColumn(outputCol, col("res").getItem(0)).drop(dropCols: _*)
      case _ =>
        throw new IllegalArgumentException(s"Option parameter '$outputOption' is not supported")
    }

  }

  def matchPhrases(dataset:Dataset[Row], props:Properties):Dataset[Row] = {

    val inputCol = props.getProperty("input.col")
    val outputCol = props.getProperty("output.col")

    val phrases = props.getProperty("phrases")
    val delimiter = props.getProperty("delimiter")

    /** DOCUMENT **/

    val document = normalizedTokens(dataset, inputCol)

    /** MATCHER **/

    val matcher = new de.kp.works.text.matching.PhraseApproach()
    matcher.setPhrases(phrases)
    matcher.setPhraseDelimiter(delimiter)

    matcher.setInputCols(Array("document", "token"))
    matcher.setOutputCol("phrase")

    val transformed = matcher.fit(document).transform(document)

    val finisher = new nlp.Finisher()
    finisher.setInputCols("phrase")
    finisher.setOutputCols(outputCol)

    finisher.transform(transformed)

  }

  def matchRegex(dataset:Dataset[Row], props:Properties):Dataset[Row] = {

    val inputCol = props.getProperty("input.col")
    val outputCol = props.getProperty("output.col")

    val rules = props.getProperty("rules")
    val delimiter = props.getProperty("delimiter")

    /** DOCUMENT **/

    val assembler = new nlp.DocumentAssembler()
    assembler.setInputCol(inputCol)
    assembler.setOutputCol("document")

    val document = assembler.transform(dataset)

    /** MATCHER **/

    val matcher = new de.kp.works.text.matching.RegexApproach()
    matcher.setRules(rules)
    matcher.setRuleDelimiter(delimiter)

    matcher.setInputCols("document")
    matcher.setOutputCol("rule")

    val transformed = matcher.fit(document).transform(document)

    val finisher = new nlp.Finisher()
    finisher.setInputCols("rule")
    finisher.setOutputCols(outputCol)

    finisher.transform(transformed)

  }


}