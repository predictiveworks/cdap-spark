package de.kp.works.ml.classification

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

import org.apache.spark.ml.classification._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

class MLPTrainer extends ClassifierTrainer {
  
  /**
   * The dataset provided has been vectorized in a previous operations
   */
  def train(vectorset:Dataset[Row], vectorCol:String, labelCol:String, params:JMap[String,Object]):MultilayerPerceptronClassificationModel = {
    /*
     * The vectorCol is an internal _vector column; it is added
     * by the vectorization operation
     */
    val classifier = new MultilayerPerceptronClassifier()

    val solver = params.get("solver").asInstanceOf[String]
    classifier.setSolver(solver)
    
    val layers = getLayers(params.get("layers").asInstanceOf[String])
    classifier.setLayers(layers)

    val blockSize = params.get("blockSize").asInstanceOf[Int]
    classifier.setBlockSize(blockSize)

    val maxIter = params.get("maxIter").asInstanceOf[Int]
    classifier.setMaxIter(maxIter)

    val stepSize = params.get("stepSize").asInstanceOf[Double]
    classifier.setStepSize(stepSize)

    val tol = params.get("tol").asInstanceOf[Double]
    classifier.setTol(tol)

    classifier.setFeaturesCol(vectorCol)
    classifier.setLabelCol(labelCol)
    
    classifier.fit(vectorset)
    
  }
  
  private def getLayers(layersStr: String):Array[Int] = {
    
    layersStr.split(",").map(layer => {
      Integer.parseInt(layer.trim)
    })
    
  }

}