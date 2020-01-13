package de.kp.works.ts
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

import de.kp.works.ts.util.MathUtils

import scala.collection.mutable.{ ArrayBuffer, WrappedArray }

trait STLParams extends TimeParams {
  /*
   * The STL decomposition is based on the grouping of the timeseries
   * data as the applied algorithm is actually no distributed algorithm.
   *
   * Therefore the 'groupCol' is required to define the technical basis
   * for the STL algorithm
   */
  final val groupCol = new Param[String](STLParams.this, "groupCol",
    "Name of the group field", (value: String) => true)

  /** @group setParam */
  def setGroupCol(value: String): this.type = set(groupCol, value)

  final val outerIter = new Param[Int](STLParams.this, "outerIter",
    "The number of cycles through the outer loop. More cycles here reduce the "
      + "affect of outliers. For most situations this can be quite small (even 0 "
      + "if there are no significant outliers). Default value is 1.", (value: Int) => true)

  /** @group setParam */
  def setOuterIter(value: Int): this.type = set(outerIter, value)

  final val innerIter = new Param[Int](STLParams.this, "innerIter",
    "The number of cycles through the inner loop. Number of cycles should "
      + " be large enough to reach convergence, which is typically only two or three. "
      + "When multiple outer cycles, the number of inner cycles can be smaller as they "
      + "do not necessarily help get overall convergence. Default value is 2.", (value: Int) => true)

  /** @group setParam */
  def setInnerIter(value: Int): this.type = set(innerIter, value)
  
  final val periodicity = new Param[Int](STLParams.this, "periodicity",
      "The periodicity of the seasonality; should be equal to lag of the "
      + "autocorrelation function with the highest (positive) correlation.",  (value: Int) => true)

  /** @group setParam */
  def setPeriodicity(value: Int): this.type = set(periodicity, value)
  
  final val seasonalLoessSize = new Param[Int](STLParams.this, "seasonalLoessSize",
      "",  (value: Int) => true)

  /** @group setParam */
  def setSeasonalLoessSize(value: Int): this.type = set(seasonalLoessSize, value)
  
  final val levelLoessSize = new Param[Int](STLParams.this, "levelLoessSize",
      "",  (value: Int) => true)

  /** @group setParam */
  def setLevelLoessSize(value: Int): this.type = set(levelLoessSize, value)
  
  final val trendLoessSize = new Param[Int](STLParams.this, "trendLoessSize",
      "",  (value: Int) => true)

  /** @group setParam */
  def setTrendLoessSize(value: Int): this.type = set(trendLoessSize, value)  
  
  setDefault(outerIter -> 1, innerIter -> 2)

}

/**
 * The goal of the algorithm is to decompose a time series Y into Y = T + S + R
 * for each point in time
 */
class STL(override val uid: String) extends Transformer with STLParams {

  def this() = this(Identifiable.randomUID("aggregate"))

  /*
   * This is the main method to perform STL decomposition
   */
  def stl_decompose(rows: WrappedArray[Row]):Array[Row] = {

    /********************
    	 * 
    	 * STL FUNCTIONS
    	 * 
    	 */
    def getRobustnessWeight(_remainder: Array[Double], _weights: Array[Double]) {

        MathUtils.getAbsolute(_remainder)

        val absRem = new Array[Double](_remainder.length)
        Array.copy(_remainder, 0, absRem, 0, _remainder.length)

        val median = MathUtils.getMedian(absRem)
        val h = 6 * median

        for (i <- 0 to _remainder.length - 1) {
          _weights(i) = MathUtils.biSquare(_remainder(i) / h)
        }

    }

    val schema = rows(0).schema

    /* STEP #1: Extract timeseries data from the provided rows */
    val sample = rows.map(row =>
      row.getDouble(schema.fieldIndex($(valueCol)))).toArray

    val sampleSize = sample.size
    /*
     * STEP #2: Prepare final data structures; the STL algorithm
     * decomposes the sample data into 3 components, seasonal,
     * trend and remainder.
     *
     * These data structures are defined empty and are iteratively
     * adjusted by the decomposition algorithm
     */
    var seasonal = Array[Double]()
    var trend = Array[Double]()
    var remainder = Array[Double]()
    /*
     * STEP #3: Define intermediate data structures, which comprise
     * robusted weights for the outer STL loop and the detrended
     * samples
     */
    var detrended = sample
    var robustness = Array.fill[Double](sampleSize)(1D)
    /*
     * STEP #4: Perform outer & inner iterations to fill decompose
     * each sample value into seasonal, trend and remainder
     */
    for (outer <- 0 to $(outerIter)) {

      if (outer > 0) {
        /* Update robustness weight for each cycle of the outer loop */
        getRobustnessWeight(remainder, robustness)
      }

      for (inner <- 0 to $(innerIter)) {
        /*
         * Iteratively calculate trend and seasonal terms:
         *
         * Detrend
         * -------
         * Y - T(inner) where 'inner' is the loop number; if the observed value Y is missing,
         * then the detrended term is also missing
         *
         * Cycle-subseries smoothing
         * -------------------------
         * The detrended time series is broken into cycle-subseries. For example, monthly data
         * (each time point describes a month) with a periodicity of twelve months would yield
         * twelve cycle-subseries, one of which would be all of the months of January.
         *
         * Each cycle-subseries is then loess smoothed. The smoothed values yield a temporary 
         * seasonal time series.
         */
        val subSeriesList = Array.ofDim[Array[Double]]($(periodicity))
        for (i <- 0 to $(periodicity) - 1) {

          /********************
         	* 
           * Extract and build seasonal subseries, 
           * e.g. all of the months of January
           * 
           */
          val subSeries = ArrayBuffer[Double]()
          val subRobustness = ArrayBuffer[Double]()

          for (j <- i to (detrended.length - 1) by $(periodicity)) {

            subSeries += detrended(j)
            subRobustness += robustness(j)

          }
          
          /* Apply LOESS smoothing */
          var subSeriesAsArray = subSeries.toArray
          var subRobustnesssAsArray = subRobustness.toArray

          MathUtils.loessSmooth(subSeriesAsArray, $(seasonalLoessSize), subRobustnesssAsArray)
          subSeriesList(i) = subSeriesAsArray
          
        }

        /* Sanity check */
        val totLength = subSeriesList.map(a => a.length).reduce((l1, l2) => l1 + l2)
        MathUtils.assertCondition(totLength == sampleSize, "[STL] Seasonal subseries total length does not match.")

        /* Reconstruct from seasonal subseries */
        val smoothedValues = new Array[Double](sampleSize)

        var vi = 0
        var i = 0

        while (vi < sampleSize) {
          subSeriesList.foreach(a => {
            if (i < a.length) {
              /* Some  sub sequences may be of shorter length */
              smoothedValues(vi) = a(i)
              vi += 1
            }
          })

          i += 1

        }

        MathUtils.assertCondition(vi == sampleSize, "[STL] Series reconstruction issue final index " + vi + " size " + sampleSize)

        /* Pad cycle at each end */
        var levelValues = new Array[Double](sampleSize + 2 * $(periodicity))

        Array.copy(smoothedValues, 0, levelValues, $(periodicity), sampleSize)
        Array.copy(smoothedValues, 0, levelValues, 0, $(periodicity))
        Array.copy(smoothedValues, sampleSize - $(periodicity), levelValues, sampleSize + $(periodicity), $(periodicity))

        /* Level with lp filter and smoothing */
        levelValues = MathUtils.lowPassFilter(levelValues, $(periodicity))
        val leOne = levelValues.length

        levelValues = MathUtils.lowPassFilter(levelValues, $(periodicity))
        val leTwo = levelValues.length

        levelValues = MathUtils.lowPassFilter(levelValues, 3)
        val leThree = levelValues.length

        MathUtils.loessSmooth(levelValues, $(levelLoessSize))
        MathUtils.assertCondition(leThree == sampleSize, "[STL] Level data size " + leThree +
          " does not match with original " + sampleSize + " length after filters " + leOne + " " + leTwo + " " + leThree)

        seasonal = MathUtils.subtractVector(smoothedValues, levelValues)
        trend = MathUtils.subtractVector(sample, seasonal)

        MathUtils.loessSmooth(trend, $(trendLoessSize), robustness)
        detrended = MathUtils.subtractVector(sample, trend)

      } /* end inner loop */

      /* Remainder */
      remainder = MathUtils.subtractVector(detrended, seasonal)
    
    } /* end outer loop */

    /* Extract average from trend */
    val average = (trend.reduce((t1, t2) => t1 + t2)) / sampleSize
    trend = trend.map(t => t - average)

    /* 
     * STEP #5: Enrich each row with the iteratively computed
     * seasonal, trend and remainder values
     */
    val enriched = rows.zipWithIndex.map{case(row,index) => {
      
      val s = seasonal(index)
      val t = trend(index)
      val r = remainder(index)
      
      val values = row.toSeq ++ Seq(s,t,r)
      Row.fromSeq(values)
    
    }}.toArray
    
    enriched
    
  }

  def transform(dataset: Dataset[_]): Dataset[Row] = {

    validateSchema(dataset.schema)
    /*
     * This transformer operates on a TimestampType column;
     * as a first step, we have to transform the dataset
     */
    val timeset = createTimeset(dataset)
    /*
     * Group dataset by groupCol and maintain all columns
     * except the group column
     */
    val cols = dataset.schema.fieldNames.filter(fieldName => fieldName != $(groupCol)).map(col)
    val aggregated = timeset.groupBy($(groupCol)).agg(collect_list(struct(cols: _*)).as("series"))
    /*
     * STL decomposition is achieved by applying an appropriate
     * UDF onto the aggregated 'series'
     */
    val stlSchema = StructType(
        /* Initial data schema except 'groupCol' */
        timeset.schema.fields.filter(f => f.name != $(groupCol)) ++ Array(
          StructField("seasonal", DoubleType, true),
          StructField("trend", DoubleType, true),
          StructField("remainder", DoubleType, true)
      ))
    
    val decompose_udf = udf(stl_decompose _, DataTypes.createArrayType(stlSchema))
    var decomposed = aggregated.withColumn("_stl_decomp", explode(decompose_udf((col("series"))))).drop("series")
    /*
     * Finally expand _stl_decomp into individual columns
     */
    stlSchema.fieldNames.foreach(fname => {
      decomposed = decomposed.withColumn(fname, col("_stl_decomp").getItem(fname))
    })
    /*
     * The initial dataset now contains extra columns, seasonal, trend
     * and remainder, that describe the result of the STL decomposition
     * for each time series value
     */
    decomposed.drop("_stl_decomp")

  }

  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  override def copy(extra: ParamMap): STL = defaultCopy(extra)

}
