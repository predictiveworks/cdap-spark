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

  def findRegressor(algoName: String, metrics: JList[RegressorMetric]): String = {
    var fsPath: String = null
    fsPath

  }

}