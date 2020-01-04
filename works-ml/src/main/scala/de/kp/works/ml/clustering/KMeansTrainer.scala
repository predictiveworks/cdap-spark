package de.kp.works.ml.clustering

/*
 * Copyright (c) 2019 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * This software is the confidential and proprietary information of 
 * Dr. Krusche & Partner PartG ("Confidential Information"). 
 * 
 * You shall not disclose such Confidential Information and shall use 
 * it only in accordance with the terms of the license agreement you 
 * entered into with Dr. Krusche & Partner PartG.
 * 
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 * 
 */
import java.util.{Map => JMap}

import org.apache.spark.ml.clustering._
import org.apache.spark.mllib.clustering.{KMeans => MLlibKMeans}

import org.apache.spark.ml.linalg.Vectors

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.collection.mutable.WrappedArray

class KMeansTrainer() {
  
  def vectorize(trainset:Dataset[Row], featuresCol:String, vectorCol:String): Dataset[Row] = {
    /*
     * The trainset contains an Array of Double value and KMeans 
     * expects a Vector representation
     */
    val vector_udf = udf {features:WrappedArray[Double] => {
      Vectors.dense(features.toArray)
    }}

    trainset.withColumn(vectorCol, vector_udf(col(featuresCol)))
    
  }
  
  def train(vectorset:Dataset[Row], vectorCol:String, params:JMap[String,Object]):KMeansModel = {
    
    val kmeans = new KMeans()
    /*
     * Parameters:
     * 
     * - k        : The number of clusters to create  
     * 
     * - initMode : This can be either "random" to choose random points as initial cluster centers, 
     *              or "k-means||" to use a parallel variant  of k-means++  (Bahmani et al., Scalable 
     *              K-Means++, VLDB 2012). Default: k-means||.
     *             
     * - initSteps: The number of steps for the k-means|| initialization mode. This is an advanced
     *              setting -- the default of 2 is almost always enough. Default: 2.
     *
     * - maxIter  : The number of iterations for the k-means algorithm
     * 
     * - tol      : The convergence tolerance for iterative algorithms
     * 
     */
    val k = params.get("k").asInstanceOf[Int]
    kmeans.setK(k)
    
    val maxIter = {
      if (params.containsKey("maxIter")) {
        params.get("maxIter").asInstanceOf[Int]
      } else 20
    }
    
    kmeans.setMaxIter(maxIter)
    
    val initSteps = {
      if (params.containsKey("initSteps")) {
        params.get("initSteps").asInstanceOf[Int]
      } else 2
    }
    
    kmeans.setInitSteps(initSteps)
    
    val tol = {
      if (params.containsKey("tolerance")) {
        params.get("tolerance").asInstanceOf[Double]
      } else 1e-4
    }
    
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