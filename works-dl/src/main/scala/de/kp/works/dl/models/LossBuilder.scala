package de.kp.works.dl.models
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

import com.typesafe.config.Config

import com.intel.analytics.zoo.pipeline.api.keras.objectives
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric.NumericFloat

import com.intel.analytics.bigdl.tensor.Tensor

trait LossBuilder extends SpecBuilder {
  
  def getLosses = List(
      "BCE", "CCE", "COS", "HIN", 
      "KLD", "MAE", "MAPE", "MSE", 
      "MSLE", "POI", "RHIN", "SCCE", 
      "SHIN")
  
  def config2Loss(loss:Config) = {

    val params = loss.getConfig("params")
    loss.getString("type") match {
      case "BCE"    => config2BCE(params)
      case "CCE"    => config2CCE(params)
      case "COS"    => config2Cosine(params)
      case "HIN"    => config2Hinge(params)
      case "KLD"    => config2KLD(params)
      case "MAE"    => config2MAE(params)
      case "MAPE"   => config2MAPE(params)
      case "MSE"    => config2MSE(params)
      case "MSLE"   => config2MSLE(params)
      case "POI"    => config2Poisson(params)
      case "RHIN"   => config2RankHinge(params)
      case "SCCE"   => config2SCCE(params)
      case "SHIN"   => config2SquaredHinge(params)
      case _ => null
    }
  }
  
  /* 
   * Binary Cross Entropy
   * 
   * This loss function measures the Binary Cross Entropy 
   * between the target and the output
   * 
   */
  private def config2BCE(params:Config) = {
     
     /* weights over the input dimension */
     val weights:Tensor[Float] = null
    /* 
     * Whether losses are averaged over observations 
     * for each mini-batch. Default is true. 
     * 
     * If false, the losses are instead summed for 
     * each mini-batch.
     */
    val sizeAverage = getAsBoolean(params, "sizeAverage", true)
    objectives.BinaryCrossEntropy(weights = weights, sizeAverage = sizeAverage) 
   
  }
  /*
   * Categorical Cross Entropy
   */
  private def config2CCE(params:Config) = {
    objectives.CategoricalCrossEntropy()
  }
  /*
   * Cosine Proximity
   */
  private def config2Cosine(params:Config) = {
    objectives.CosineProximity()
  }
  /*
   * Hinge
   */
  private def config2Hinge(params:Config) = {
    
    /* Margin */
    val margin = getAsDouble(params, "margin", 1D)
    /* 
     * Whether losses are averaged over observations 
     * for each mini-batch. Default is true. 
     * 
     * If false, the losses are instead summed for 
     * each mini-batch.
     */
    val sizeAverage = getAsBoolean(params, "sizeAverage", true)    
    objectives.Hinge(margin = margin, sizeAverage = sizeAverage)
  } 
  /*
   * Kullback Leibler Divergence
   */
  private def config2KLD(params:Config) = {
    objectives.KullbackLeiblerDivergence()
  }
  /*
   * Mean Absolute Error
   */
  private def config2MAE(params:Config) = {
    
    /* 
     * Whether losses are averaged over observations 
     * for each mini-batch. Default is true. 
     * 
     * If false, the losses are instead summed for 
     * each mini-batch.
     */
    val sizeAverage = getAsBoolean(params, "sizeAverage", true)
    objectives.MeanAbsoluteError(sizeAverage)

  }
  /*
   * Mean Absolute Percentage Error
   */
  private def config2MAPE(params:Config) = {
    objectives.MeanAbsolutePercentageError()
  }
  /*
   * Mean Squared Error
   */
  private def config2MSE(params:Config) = {
    
    /* 
     * Whether losses are averaged over observations 
     * for each mini-batch. Default is true. 
     * 
     * If false, the losses are instead summed for 
     * each mini-batch.
     */
    val sizeAverage = getAsBoolean(params, "sizeAverage", true)
    objectives.MeanSquaredError(sizeAverage)

  }
  /*
   * Mean Squared Logarithmic Error
   */
  private def config2MSLE(params:Config) = {
    objectives.MeanSquaredLogarithmicError()
  }
  /*
   * Poisson
   */
  private def config2Poisson(params:Config) = {
    objectives.Poisson()
  }
  /*
   * RankHinge
   */
  private def config2RankHinge(params:Config) = {
    
    /* Margin */
    val margin = getAsDouble(params, "margin", 1D)
    objectives.RankHinge(margin = margin)
  } 
  /*
   * SparseCategoricalCrossEntropy
   */
  private def config2SCCE(params:Config) = {
    
    /* Whether to accept log-probabilities or probabilities */
    val logProbAsInput = getAsBoolean(params, "logProbAsInput", false)
    
    /* 
     * Whether target labels start from 0. Default is true.
		 * If false, labels start from 1.
		 */
    val zeroBasedLabel = getAsBoolean(params, "zeroBasedLabel", true)    
    /* 
     * Whether losses are averaged over observations 
     * for each mini-batch. Default is true. 
     * 
     * If false, the losses are instead summed for 
     * each mini-batch.
     */
    val sizeAverage = getAsBoolean(params, "sizeAverage", true)
            
    /* 
     * Weights of each class if you have an unbalanced training set.
		 * Default is null.
		 * 
		 * Not supported yet
     */
    val weights:Tensor[Float] = null
    /*
     * If the target is set to this value, the training process
     * will skip this sample. In other words, the forward process 
     * will return zero output and the backward process will also 
     * return zero gradInput. Default is -1.
     */
    val paddingValue = getAsInt(params, "paddingValue", -1)
    objectives.SparseCategoricalCrossEntropy(
        logProbAsInput = logProbAsInput,
        zeroBasedLabel = zeroBasedLabel,
        sizeAverage = sizeAverage,
        paddingValue = paddingValue)
  }
  /*
   * Squared Hinge
   */
  private def config2SquaredHinge(params:Config) = {
    
    /* Margin */
    val margin = getAsDouble(params, "margin", 1D)
    /* 
     * Whether losses are averaged over observations 
     * for each mini-batch. Default is true. 
     * 
     * If false, the losses are instead summed for 
     * each mini-batch.
     */
    val sizeAverage = getAsBoolean(params, "sizeAverage", true)    
    objectives.SquaredHinge(margin = margin, sizeAverage = sizeAverage)
  } 

}
