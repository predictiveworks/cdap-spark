package de.kp.works.ml.clustering
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

import org.apache.spark.ml.clustering._
import org.apache.spark.sql._

class BisectingKMeansTrainer extends ClusterTrainer {
  
  def train(vectorset:Dataset[Row], vectorCol:String, params:JMap[String,Object]):BisectingKMeansModel = {
    
    val bisectingKMeans = new BisectingKMeans()
    
    val k = params.get("k").asInstanceOf[Int] 
    bisectingKMeans.setK(k)
    
    val maxIter = params.get("maxIter").asInstanceOf[Int] 
    bisectingKMeans.setMaxIter(maxIter)
     
    val minDivisibleClusterSize = params.get("minDivisibleClusterSize").asInstanceOf[Double] 
    bisectingKMeans.setMinDivisibleClusterSize(minDivisibleClusterSize)
     
    bisectingKMeans.setFeaturesCol(vectorCol)
    bisectingKMeans.fit(vectorset)
    
  }

}