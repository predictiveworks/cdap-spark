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

import java.util.{Map => JMap}

import org.apache.spark.ml.regression._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

class RFRegressor {
      
  def train(trainset:Dataset[Row], featuresCol:String, labelCol:String, params:JMap[String,Object]):RandomForestRegressionModel = {
    /*
     * The vectorCol is an internal _vector column; it is added
     * by the vectorization operation
     */
    val regressor = new RandomForestRegressor()

    val maxBins = params.get("maxBins").asInstanceOf[Int]
    regressor.setMaxBins(maxBins)
    
    val maxDepth = params.get("maxDepth").asInstanceOf[Int]
    regressor.setMaxDepth(maxDepth)
    
    val minInfoGain = params.get("minInfoGain").asInstanceOf[Double]
    regressor.setMinInfoGain(minInfoGain)
    
    val numTrees = params.get("numTrees").asInstanceOf[Int]
    regressor.setNumTrees(numTrees)

    regressor.setFeaturesCol(featuresCol)
    regressor.setLabelCol(labelCol)
    
    regressor.fit(trainset)
 
  }

}