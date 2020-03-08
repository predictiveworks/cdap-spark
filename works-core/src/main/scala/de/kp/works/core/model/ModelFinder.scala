package de.kp.works.core.model
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

import java.util.{ List => JList }
import de.kp.works.core.Algorithms

import scala.collection.JavaConversions._

object ModelFinder {

  def findClassifier(algoName: String, metrics: JList[ClassifierMetric]): String = {
    var fsPath: String = null
    fsPath
  }

  def findCluster(algoName: String, metrics: JList[ClusterMetric]): String = {

    var fsPath:String = null
    algoName match {
      case 
      Algorithms.BISECTING_KMEANS | 
      Algorithms.GAUSSIAN_MIXTURE | 
      Algorithms.KMEANS => {
        /*
			   * This cluster algorithms are evaluated using the silhouette measure
			 	 * provided by Apache Spark ML:
			   *
			   * The Silhouette is a measure for the validation of the consistency
			   * within clusters. It ranges between 1 and -1, where a value close
			   * to 1 means that the points in a cluster are close to the other points
			   * in the same cluster and far from the points of the other clusters.
			   *
			   * In order to find the best model instance, we therefore look for
			   * the maximum value. The current implementation is limited to the
			   * euclidean distance to determine the best cluster model
			   */
        val ary = metrics
          .map(metric => (metric.fsPath, metric.silhouette_euclidean)).toArray
         
        val maximum = ary.sortBy(_._2).last
        if (maximum._2 > 0) fsPath = maximum._1
       
      }
      case Algorithms.LATENT_DIRICHLET_ALLOCATION => {
      }

    }

    fsPath

  }
  /*
   * This method finds the best model by taking all registered
   * regression metrics (rsme, mae, mse and r2) into account.
   * 
   * Each metric value is compared with the respective best 
   * (minimum or maximum value) and scaled with the maximum.
   * 
   * The resulting scaled deviation errors are summed into 
   * a single value and the best model is determined from
   * the minimum value of the summed error
   */
  def findRegressor(algoName: String, metrics: JList[RegressorMetric]): String = {
    
    var fsPath: String = null
    /*
     * STEP #1: Determine the minimum and maximum values 
     * for each metric metric to build normalized metric 
     * values
     */
    val (rsme_min, rsme_max) = {

      val rsme = metrics.map(_.rsme).toArray
      (rsme.min, rsme.max)

    }
    
    val (mae_min, mae_max) = {

      val mae = metrics.map(_.mae).toArray
      (mae.min, mae.max)

       
    }
    
    val (mse_min, mse_max) = {

      val mse = metrics.map(_.mse).toArray
      (mse.min, mse.max)

       
    }
    
    val (r2_min, r2_max) = {

      val r2 = metrics.map(_.r2).toArray
      (r2.min, r2.max)

       
    }
    /*
     * STEP #2: Normalize and aggregate each metric
     * value and build sum of normalize metric 
     */
    val scaled = metrics.map(metric => {
      
      /* RSME: The smallest scaled deviation from the 
       * minimum value is best  
       */
      val rsme = 
        if (rsme_max == 0D) 0D else Math.abs((rsme_min - metric.rsme) / rsme_max)
      
      /* MAE: The smallest scaled deviation from the 
       * minimum value is best  
       */
      val mae = 
        if (mae_max == 0D) 0D else Math.abs((mae_min - metric.mae) / mae_max)
      
      /* MSE: The smallest scaled deviation from the 
       * minimum value is best  
       */
      val mse = 
        if (mse_max == 0D) 0D else Math.abs((mse_min - metric.mse) / mse_max)
      
      /* R2: The smallest scaled deviation from the 
       * maximum value is best  
       */
      val r2 = 
        if (r2_max == 0D) 0D else Math.abs((r2_min - metric.r2) /r2_max)
       
      val err = rsme + mae + mse + r2
      (metric.fsPath, err)
      
    }).toArray
         
    val minimum = scaled.sortBy(_._2).head
    fsPath = minimum._1
    
    fsPath

  }

}