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
import de.kp.works.core.Names

import scala.collection.JavaConversions._

trait MinMaxFinder {
  
  def classifierMinMax(metric:String, metrics:JList[ClassifierMetric]): (Double, Double) = {
    
    metric match {
      case Names.ACCURACY =>
        val values = metrics.map(_.accuracy).toArray
        (values.min, values.max)

      case Names.F1 =>
        val values = metrics.map(_.f1).toArray
        (values.min, values.max)

      case Names.WEIGHTED_FMEASURE =>
        val values = metrics.map(_.weightedFMeasure).toArray
        (values.min, values.max)

      case Names.WEIGHTED_PRECISION =>
        val values = metrics.map(_.weightedPrecision).toArray
        (values.min, values.max)

      case Names.WEIGHTED_RECALL =>
        val values = metrics.map(_.weightedRecall).toArray
        (values.min, values.max)

      case Names.WEIGHTED_FALSE_POSITIVE =>
        val values = metrics.map(_.weightedFalsePositiveRate).toArray
        (values.min, values.max)

      case Names.WEIGHTED_TRUE_POSITIVE =>
        val values = metrics.map(_.weightedTruePositiveRate).toArray
        (values.min, values.max)

      case _ => throw new IllegalArgumentException("Unknown classifier metric detected.")
    }
    
  }
  
  def clusterMinMax(metric:String, metrics:JList[ClusterMetric]):(Double, Double) = {
    
    metric match {
      
      case Names.LIKELIHOOD =>
        val values = metrics.map(_.likelihood).toArray
        (values.min, values.max)

      case Names.PERPLEXITY =>
        val values = metrics.map(_.perplexity).toArray
        (values.min, values.max)

      case Names.SILHOUETTE_COSINE =>
        val values = metrics.map(_.silhouette_cosine).toArray
        (values.min, values.max)

      case Names.SILHOUETTE_EUCLIDEAN =>
        val values = metrics.map(_.silhouette_euclidean).toArray
        (values.min, values.max)

      case _ => throw new IllegalArgumentException("Unknown cluster metric detected.")      
    }
    
  }
  
  def regressorMinMax(metric:String, metrics:JList[RegressorMetric]): (Double, Double) = {
    
    metric match {
      case Names.MAE =>
        val values = metrics.map(_.mae).toArray
        (values.min, values.max)

      case Names.MSE =>
        val values = metrics.map(_.mse).toArray
        (values.min, values.max)

      case Names.R2 =>
        val values = metrics.map(_.r2).toArray
        (values.min, values.max)

      case Names.RSME =>
        val values = metrics.map(_.rsme).toArray
        (values.min, values.max)

      case _ => throw new IllegalArgumentException("Unknown regressor metric detected.")
    }
    
  }

}