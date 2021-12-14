package de.kp.works.core.model
/*
 * Copyright (c) 2019 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
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
import de.kp.works.core.{Algorithms, Names}

import scala.collection.JavaConversions._

object ModelFinder extends MinMaxFinder {
  
  /*
   * This method finds the best model by taking all registered
   * classifier metrics into account.
   * 
   * Each metric value is compared with the respective best 
   * (minimum or maximum value) and scaled with the maximum.
   * 
   * The resulting scaled deviation errors are summed into 
   * a single value and the best model is determined from
   * the minimum value of the summed error
   */
  def findClassifier(algoName: String, metrics: JList[ClassifierMetric]): ModelProfile = {
    
    /*
     * STEP #1: Determine the minimum and maximum values 
     * for each metric metric to build normalized metric 
     * values
     */
    val (_, accuracy_max) = classifierMinMax(Names.ACCURACY, metrics)

    val (_, f1_max) = classifierMinMax(Names.F1, metrics)
    val (_, weightedFMeasure_max) = classifierMinMax(Names.WEIGHTED_FMEASURE, metrics)
    
    val (_, weightedPrecision_max) = classifierMinMax(Names.WEIGHTED_PRECISION, metrics)
    val (_, weightedRecall_max) = classifierMinMax(Names.WEIGHTED_RECALL, metrics)
    /* 
     * Weighted TPR is equivalent to WeightedRecall and must not
     * be counted twice
     */
    val (_, weightedFPR_max) = classifierMinMax(Names.WEIGHTED_FALSE_POSITIVE, metrics)
    /*
     * STEP #2: Normalize and aggregate each metric
     * value and build sum of normalize metric 
     */
    val scaled = metrics.map(metric => {
      
      /* ACCURACY: The smallest scaled deviation from the 
       * maximum value is best  
       */
      val accuracy = 
        if (accuracy_max == 0D) 0D else Math.abs((accuracy_max - metric.accuracy) / accuracy_max)
        
      /* F1: The smallest scaled deviation from the 
       * maximum value is best  
       */
      val f1 = 
        if (f1_max == 0D) 0D else Math.abs((f1_max - metric.f1) / f1_max)

      /* WEIGHTED F1: The smallest scaled deviation from the 
       * maximum value is best  
       */
      val weightedFMeasure = 
        if (weightedFMeasure_max == 0D) 0D else Math.abs((weightedFMeasure_max - metric.weightedFMeasure) / weightedFMeasure_max)
 
      /* WEIGHTED PRECISION: The smallest scaled deviation from the 
       * maximum value is best  
       */
      val weightedPrecision = 
        if (weightedPrecision_max == 0D) 0D else Math.abs((weightedPrecision_max - metric.weightedPrecision) / weightedPrecision_max)
 
      /* WEIGHTED RECALL: The smallest scaled deviation from the 
       * maximum value is best  
       */
      val weightedRecall = 
        if (weightedRecall_max == 0D) 0D else Math.abs((weightedRecall_max - metric.weightedRecall) / weightedRecall_max)
 
      /* WEIGHTED FPR: The smallest scaled deviation from the 
       * minimum value is best  
       */
      val weightedFPR = 
        if (weightedFPR_max == 0D) 0D else Math.abs((weightedFPR_max - metric.weightedFalsePositiveRate) / weightedFPR_max)
        
      val err = accuracy + f1 + weightedFMeasure + weightedPrecision + weightedRecall + weightedFPR
      (metric.fsPath, metric.id, err)
      
    }).toArray
    /*
     * Determine the best model as that model that has the 
     * lowest aggregated metric value             
     */
    val sorted = scaled.sortBy(_._3)
    val best = sorted.head
    /*
     * Each model is described by a 'trustability' value that
     * normalizes the aggregated metric value
     */
    val min = best._3
    val max = sorted.last._3
    
    val trust = if (max == 0D) 1D else 1 - min / max
    
    val profile = new ModelProfile()
      .setId(best._2)
      .setPath(best._1)
      .setTrustability(trust)

    profile
    
  }

  def findCluster(algoName: String, metrics: JList[ClusterMetric]): ModelProfile = {

    algoName match {
      case 
      Algorithms.BISECTING_KMEANS | 
      Algorithms.GAUSSIAN_MIXTURE | 
      Algorithms.KMEANS =>
        /*
         * STEP #1: Determine the minimum and maximum values
         * for each metric metric to build normalized metric
         * values
         */
        val (_, cosine_max) = clusterMinMax(Names.SILHOUETTE_COSINE, metrics)
        val (_, euclidean_max) = clusterMinMax(Names.SILHOUETTE_EUCLIDEAN, metrics)
        /*
         * STEP #2: Normalize and aggregate each metric
         * value and build sum of normalize metric
         */
        val scaled = metrics.map(metric => {

          /* COSINE: The smallest scaled deviation from the
           * maximum value is best
           */
          val cosine =
            if (cosine_max == 0D) 0D else Math.abs((cosine_max - metric.silhouette_cosine) / cosine_max)

          /* EUCLIDEAN: The smallest scaled deviation from the
           * maximum value is best
           */
          val euclidean =
            if (euclidean_max == 0D) 0D else Math.abs((euclidean_max - metric.silhouette_euclidean) / euclidean_max)

          val err = cosine + euclidean
          (metric.fsPath, metric.id, err)

        }).toArray

        /*
         * Determine the best model as that model that has the
         * lowest aggregated metric value
         */
        val sorted = scaled.sortBy(_._3)
        val best = sorted.head
        /*
         * Each model is described by a 'trustability' value that
         * normalizes the aggregated metric value
         */
        val min = best._3
        val max = sorted.last._3

        val trust = if (max == 0D) 1D else 1 - min / max

        val profile = new ModelProfile()
          .setId(best._2)
          .setPath(best._1)
          .setTrustability(trust)

        profile
      case Algorithms.LATENT_DIRICHLET_ALLOCATION =>
        /*
         * STEP #1: Determine the minimum and maximum values
         * for each metric metric to build normalized metric
         * values
         */
        val (likelihood_min, likelihood_max) = clusterMinMax(Names.LIKELIHOOD, metrics)
        val (perplexity_min, perplexity_max) = clusterMinMax(Names.PERPLEXITY, metrics)
        /*
         * STEP #2: Normalize and aggregate each metric
         * value and build sum of normalize metric
         */
        val scaled = metrics.map(metric => {

          /* LIKELIHOOD: The smallest scaled deviation from the
           * minimum value is best
           */
          val likelihood =
            if (likelihood_max == 0D) 0D else Math.abs((likelihood_min - metric.likelihood) / likelihood_max)

          /* PERPLEXITY: The smallest scaled deviation from the
           * minimum value is best
           */
          val perplexity =
            if (perplexity_max == 0D) 0D else Math.abs((perplexity_min - metric.perplexity) / perplexity_max)

          val err = likelihood + perplexity
          (metric.fsPath, metric.id, err)

        }).toArray

        /*
         * Determine the best model as that model that has the
         * lowest aggregated metric value
         */
        val sorted = scaled.sortBy(_._3)
        val best = sorted.head
        /*
         * Each model is described by a 'trustability' value that
         * normalizes the aggregated metric value
         */
        val min = best._3
        val max = sorted.last._3

        val trust = if (max == 0D) 1D else 1 - min / max

        val profile = new ModelProfile()
          .setId(best._2)
          .setPath(best._1)
          .setTrustability(trust)

        profile
    }

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
  def findRegressor(algoName: String, metrics: JList[RegressorMetric]): ModelProfile = {
    
    /*
     * STEP #1: Determine the minimum and maximum values 
     * for each metric metric to build normalized metric 
     * values
     */
    val (mae_min, mae_max)   = regressorMinMax(Names.MAE, metrics)    
    val (mse_min, mse_max)   = regressorMinMax(Names.MSE, metrics)    
    val (r2_min, r2_max)     = regressorMinMax(Names.R2, metrics)
    val (rsme_min, rsme_max) = regressorMinMax(Names.RSME, metrics)    
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
      (metric.fsPath, metric.id, err)
      
    }).toArray
    
    /*
     * Determine the best model as that model that has the 
     * lowest aggregated metric value             
     */
    val sorted = scaled.sortBy(_._3)
    val best = sorted.head
    /*
     * Each model is described by a 'trustability' value that
     * normalizes the aggregated metric value
     */
    val min = best._3
    val max = sorted.last._3
    
    val trust = if (max == 0D) 1D else 1 - min / max
    
    val profile = new ModelProfile()
      .setId(best._2)
      .setPath(best._1)
      .setTrustability(trust)

      profile
    

  }

}