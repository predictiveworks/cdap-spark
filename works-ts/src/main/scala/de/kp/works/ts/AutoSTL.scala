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

trait AutoSTLParams extends STLParams {

  /*
   * This auto correlation function computes values for a sequence of 
   * lag values either specified by its max lag, i.e. 1 <= k <= max_lag,
   * or, by a discrete list of lag values
   */
  final val maxLag = new Param[Int](AutoSTLParams.this, "maxLag",
      "The maximum lag value. Default is 1.", (value:Int) => true)

  final val lagValues = new Param[Array[Int]](AutoSTLParams.this, "lagValues",
      "The sequence of lag value. This sequence is empty by default.", (value:Array[Int]) => true)

  final val threshold = new Param[Double](AutoSTLParams.this, "threshold",
      "The threshold used to determine the lag value with the highest correlation score. "
      + "Default is 0.95.", (value:Double) => true)    

  /** @group setParam */
  def setMaxLag(value:Int): AutoSTLParams.this.type = set(maxLag, value)

  /** @group setParam */
  def setLagValues(value:Array[Int]): AutoSTLParams.this.type = set(lagValues, value)

  /** @group setParam */
  def setThreshold(value:Double): AutoSTLParams.this.type = set(threshold, value)
  
  setDefault(maxLag -> 1, lagValues -> Array.empty[Int])
}

class AutoSTL(override val uid: String) extends Transformer with AutoSTLParams {

  def this() = this(Identifiable.randomUID("autoSTLDecompose"))

  def transform(dataset: Dataset[_]): Dataset[Row] = {

    validateSchema(dataset.schema)
    /*
     * STEP #1: Compute Auto Correlation Function
     */
    val autoCorrelation = new AutoCorrelation()
		autoCorrelation.setValueCol($(valueCol))

		autoCorrelation.setThreshold($(threshold))
		if ($(lagValues).isEmpty)
			autoCorrelation.setMaxLag($(maxLag))

		else
			autoCorrelation.setLagValues($(lagValues))
    
		val seasonalPeriod = autoCorrelation.fit(dataset).getSeasonalPeriod
		setPeriodicity(seasonalPeriod)
    
    val stl = new STL()
    stl
      .setTimeCol($(timeCol))
      .setValueCol($(valueCol))
      .setGroupCol($(groupCol))
      .setOuterIter($(outerIter))
      .setInnerIter($(innerIter))
      .setPeriodicity($(periodicity))
      .setSeasonalLoessSize($(seasonalLoessSize))
      .setLevelLoessSize($(levelLoessSize))
      .setTrendLoessSize($(trendLoessSize))
      
    stl.transform(dataset)
    
  }

  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  override def copy(extra: ParamMap): AutoSTL = defaultCopy(extra)

  
}