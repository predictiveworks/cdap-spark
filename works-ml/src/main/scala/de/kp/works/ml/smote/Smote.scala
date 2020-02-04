package de.kp.works.ml.smote
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

import org.apache.spark.ml
import org.apache.spark.ml.feature._
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import de.kp.works.ml.util._

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * Runs the SMOTE algorithm to generate synthetic examples of the minority class.
  * This increases the size of the minority class to solve the imbalanced class problem.
  *
  * The synthetic samples are generated in the following way:
  * 
  * (1) Randomly choose one of k neighbours for each sample in the minority class.
  * 
  * (2) Take the difference of the feature vector between the sample and its neighbour.
  * 
  * (3) Multiply this difference by a random factor in the range [0, 1], and the result
  *     is the synthetic example.
  *
  * For discrete attributes, the synthetic example randomly picks either the original sample 
  * or the neighbour, and copies that value.
  *
  * The k-nearest neighbours are approximated by the Locality Sensitive Hashing (LSH) model
  * in Spark ML.
  *
  * @see Chawla, N. V., Bowyer, K. W., Hall, L. O., & Kegelmeyer, W. P. (2002).
  * 
  * SMOTE: Synthetic Minority Over-sampling Eechnique. Journal of artificial 
  * intelligence research, 16, 321-357.
  *      
  */
final case class Smote(
    /*
     * A dataset that represents a certain label in the
     * training dataset of a classification or regression
     * algorithm, that must be filled up with synthetic
     * records
     */
    sample: Dataset[_],
    /*
     * The SMOTE algorithm leverages the Bucketed LSH and
     * this algorithm needs feature vectors (as a single
     * column)
     */
    featuresCol: String,
    
    /*** Bucketed LSH ***/

    /*
     * The SMOTE algorithm requires a dissembled set of
     * features e.g. of the form f_0, f_1, etc
     */
    featureCols: Seq[String],
    /*
     * The length of each bucket
     */
    bucketLength: Option[Double] = None,
    /*
     * The number of hash tables
     */
    numHashTables: Int = 1,
    
    /*** SMOTE ***/
    
    /*
     * Number of synthetic examples to create, per example 
     * in the sample, i.e. in case of 2, the number of minor
     * examples will be 3
     */
    sizeMultiplier: Int = 2,
    numNearestNeighbours: Int = 4,
    seed: Option[Int] = None)(implicit spark: SparkSession) extends Log4jLogger {

  require(sample.count != 0, "sample must not be empty")
  require(numHashTables >= 1, "number of hash tables must be greater than or equals 1")
  require(sizeMultiplier >= 2, "size multiplier must be greater than or equals 2")
  require(numNearestNeighbours >= 1, "number of nearest neighbours must be greater than or equals 1")

  private implicit val rand: Random = seed match {
    case Some(s) => new Random(s)
    case _ => new Random()
  }

  private val allAttributes: Seq[String] =  featureCols

  require(allAttributes.nonEmpty, "there must be at least one attribute")

  private val outSchema: StructType = StructType(
      featureCols.map(x => StructField(x, DoubleType, nullable = false))
  )

  implicit private val encoder: ExpressionEncoder[Row] = RowEncoder(outSchema)

  private val blen: Double = bucketLength match {
    case Some(x) =>
      require(x > 0, "bucket length must be greater than zero")
      x
    case _ => LSHUtil.bucketLength(sample.count, allAttributes.length)
  }
  /*
   * This is a helper method to prepare the Bucketed LSH
   */
  private val bucketedLSH: BucketedRandomProjectionLSH = {
    
    val res = new BucketedRandomProjectionLSH()
      .setInputCol(featuresCol)
      .setBucketLength(blen)
      .setNumHashTables(numHashTables)
    
      seed match {
      case Some(s) => res.setSeed(s)
      case _ => res
    
    }
  
  }

  private def syntheticExample(base: Row, neighbour: Row): Row = {
    var res: Row = base
    for (c <- featureCols) {
      res = Smote.setContinuousAttribute(c, res, neighbour)
    }
    res
  }

  /**
    * Uses LSH from Spark ML to approximate k-nearest neighbours for each example in the input sample
    * (also known as the key).
    *
    * @example For a 2-column input, the resulting schema looks like: key_col1, key_col2, neighbours_col1 (list), neighbours_col2 (list)
    * @param keyColumnPrefix prefix for the key column name
    * @return k-nearest neighbours of each key
    */
  private def nearestNeighbours(keyColumnPrefix: String): DataFrame = {
    
    /*
     * This is the first step to transform the initial dataset
     * into its bucketed LSH transformation
     */
    val bucketedLSHModel = bucketedLSH.fit(sample)
    val bucketedDataset = bucketedLSHModel.transform(sample)
    
    val distCol: String = "_smote_distance"
    val keyCols: String = allAttributes map (x => s"$keyColumnPrefix$x") mkString ","
    val columnIndices: String = Seq.range(1, 2 * allAttributes.length + 1) mkString ","
    val collectListCols: String = allAttributes.map(x => s"$x) $x").mkString(
      "COLLECT_LIST(", ",COLLECT_LIST(", "")
    val datasetACols: String = allAttributes map (x => s"datasetA.$x $keyColumnPrefix$x") mkString ","
    val datasetBCols: String = allAttributes map (x => s"datasetB.$x $x") mkString ","
    val viewName: String = "_smote_similarity_matrix_raw"
    val sql: String =
      s"""
         |select $keyCols
         |,$collectListCols
         |from (
         |select $keyCols
         |,${allAttributes mkString ","}
         |,ROW_NUMBER() OVER (PARTITION BY $keyCols ORDER BY $distCol) as rank
         |from (
         |select $datasetACols
         |,$datasetBCols
         |,avg($distCol) $distCol
         |from $viewName
         |group by $columnIndices
         |) t1 ) t2
         |where rank<=$numNearestNeighbours
         |group by $keyCols
       """.stripMargin
    log.trace(sql)
    bucketedLSHModel.approxSimilarityJoin(bucketedDataset, bucketedDataset, threshold = Double.MaxValue, distCol = distCol)
      .createOrReplaceTempView(viewName)
    spark.sql(sql)
    
  }

  private def neighbour(row: Row): Row = {
    val i: Int = allAttributes.headOption match {
      case Some(a) => rand.nextInt(row.getAs[Seq[Any]](a).length)
      case _ => 0
    }
    val vs: ArrayBuffer[Any] = ArrayBuffer()
    for (a <- featureCols) {
      vs += row.getAs[Seq[Double]](a)(i)
    }
    new GenericRowWithSchema(vs.toArray, outSchema)
  }

  private def key(row: Row, keyColumnPrefix: String): Row = {
    val vs: ArrayBuffer[Any] = ArrayBuffer()
    for (a <- featureCols) {
      vs += row.getAs[Double](s"$keyColumnPrefix$a")
    }
    new GenericRowWithSchema(vs.toArray, outSchema)
  }

  /**
    * Generates the synthetic examples as a DataFrame.
    *
    * The schema is exactly the union of the discreteStringAttributes,
    * discreteLongAttributes and continuousAttributes that were passed in as constructor arguments.
    *
    * The size of the result should equal the size of the input sample multiplied by the sizeMultiplier.
    *
    * @return the synthetic examples
    */
  def syntheticSample: DataFrame = {
    
    val keyColumnPrefix: String = "_smote_key_"
    val knn: DataFrame = nearestNeighbours(keyColumnPrefix)
    knn flatMap { row =>
      val arr: ArrayBuffer[Row] = ArrayBuffer()
      for (_ <- 0 until sizeMultiplier) {
        arr += syntheticExample(base = key(row, keyColumnPrefix),neighbour = neighbour(row))
      }
      arr
    }
  }.selectExpr(allAttributes: _*)
}

object Smote {
  
  private def setContinuousAttribute(name: String, base: Row, neighbour: Row)
    (implicit rand: Random): Row = {
    
      val lc: Double = base.getAs[Double](name)
      val rc: Double = neighbour.getAs[Double](name)
      val diff: Double = rc - lc
      val gap: Double = rand.nextDouble()
      val newValue: Double = lc + (gap * diff)

      SparkUtil.update(base, name, newValue)
    }

  private def setDiscreteAttribute[T](name: String, base: Row, neighbour: Row)
    (implicit rand: Random): Row = {    
    
      val ld = base.getAs[T](name)
    
      val rd = neighbour.getAs[T](name)
      val newValue = rand.nextInt(2) match {
        case 0 => ld
        case _ => rd
      }
    
      SparkUtil.update(base, name, newValue)
      
    }
}