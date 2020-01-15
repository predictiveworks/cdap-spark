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
import org.apache.spark.mllib.clustering.{KMeans => MLlibKMeans}

import org.apache.spark.sql._

class KMeansTrainer() extends ClusterTrainer {
  
  def train(vectorset:Dataset[Row], vectorCol:String, params:JMap[String,Object]):KMeansModel = {
    
    val kmeans = new KMeans()

    val k = params.get("k").asInstanceOf[Int]
    kmeans.setK(k)
    
    val maxIter = params.get("maxIter").asInstanceOf[Int]
    kmeans.setMaxIter(maxIter)
    
    val initSteps = params.get("initSteps").asInstanceOf[Int]
    kmeans.setInitSteps(initSteps)
    
    val tol = params.get("tolerance").asInstanceOf[Double]
    kmeans.setTol(tol)

    val initMode = {
      if (params.containsKey("initMode")) {

        val mode = params.get("initMode").asInstanceOf[String]        
        if (mode == "parallel")
          MLlibKMeans.K_MEANS_PARALLEL
         
        else if (mode == "random")
          MLlibKMeans.RANDOM
        
        else 
          throw new Exception(s"[KMeansTrainer] initMode '$mode' is not supported.")

      } else MLlibKMeans.K_MEANS_PARALLEL
    
    }
  
    kmeans.setInitMode(initMode)
    /*
     * The KMeans trainer determines the KMeans cluster model (fit) 
     * and does not depend on the prediction column    
     */
    kmeans.setFeaturesCol(vectorCol)
    kmeans.fit(vectorset)

  }
}