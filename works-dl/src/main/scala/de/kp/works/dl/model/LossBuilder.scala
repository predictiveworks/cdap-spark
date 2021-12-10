package de.kp.works.dl.model
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

import com.intel.analytics.bigdl.tensor.Tensor
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric
import com.intel.analytics.zoo.pipeline.api.keras.objectives
import com.intel.analytics.zoo.pipeline.api.keras.objectives.{BinaryCrossEntropy, CategoricalCrossEntropy, CosineProximity, Hinge, KullbackLeiblerDivergence, MeanAbsoluteError, MeanAbsolutePercentageError, MeanSquaredError, MeanSquaredLogarithmicError, Poisson, RankHinge, SparseCategoricalCrossEntropy, SquaredHinge, TensorLossFunction}
import com.typesafe.config.Config

trait LossBuilder extends SpecBuilder {

  def getLosses = List(
      "BCE", "CCE", "COS", "HIN",
      "KLD", "MAE", "MAPE", "MSE",
      "MSLE", "POI", "RHIN", "SCCE",
      "SHIN")

  def config2Loss(loss:Config)
                 (implicit ev: TensorNumeric[Float]): TensorLossFunction[Float] = {

    val params = loss.getConfig(ModelNames.PARAMS)
    loss.getString(ModelNames.TYPE) match {
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
  def config2BCE(params:Config)
                (implicit ev: TensorNumeric[Float]): BinaryCrossEntropy[Float] = {

     /* weights over the input dimension */
     val weights:Tensor[Float] = null
    /*
     * Whether losses are averaged over observations
     * for each mini-batch. Default is true.
     *
     * If false, the losses are instead summed for
     * each mini-batch.
     */
    val sizeAverage = getAsBoolean(params, ModelNames.SIZE_AVERAGE, default = true)
    objectives.BinaryCrossEntropy(weights = weights, sizeAverage = sizeAverage)

  }
  /*
   * Categorical Cross Entropy
   */
  def config2CCE(params:Config)
                        (implicit ev: TensorNumeric[Float]): CategoricalCrossEntropy[Float] = {
    objectives.CategoricalCrossEntropy()
  }
  /*
   * Cosine Proximity
   */
  def config2Cosine(params:Config)
                   (implicit ev: TensorNumeric[Float]): CosineProximity[Float] = {
    objectives.CosineProximity()
  }
  /*
   * Hinge
   */
  def config2Hinge(params:Config)
                  (implicit ev: TensorNumeric[Float]): Hinge[Float] = {

    /* Margin */
    val margin = getAsDouble(params, ModelNames.MARGIN, 1D)
    /*
     * Whether losses are averaged over observations
     * for each mini-batch. Default is true.
     *
     * If false, the losses are instead summed for
     * each mini-batch.
     */
    val sizeAverage = getAsBoolean(params, ModelNames.SIZE_AVERAGE, default = true)
    objectives.Hinge(margin = margin, sizeAverage = sizeAverage)
  }
  /*
   * Kullback Leibler Divergence
   */
  def config2KLD(params:Config)
                (implicit ev: TensorNumeric[Float]): KullbackLeiblerDivergence[Float] = {
    objectives.KullbackLeiblerDivergence()
  }
  /*
   * Mean Absolute Error
   */
  def config2MAE(params:Config)
                (implicit ev: TensorNumeric[Float]): MeanAbsoluteError[Float] = {

    /*
     * Whether losses are averaged over observations
     * for each mini-batch. Default is true.
     *
     * If false, the losses are instead summed for
     * each mini-batch.
     */
    val sizeAverage = getAsBoolean(params, ModelNames.SIZE_AVERAGE, default = true)
    objectives.MeanAbsoluteError(sizeAverage)

  }
  /*
   * Mean Absolute Percentage Error
   */
  def config2MAPE(params:Config)
                 (implicit ev: TensorNumeric[Float]): MeanAbsolutePercentageError[Float] = {
    objectives.MeanAbsolutePercentageError()
  }
  /*
   * Mean Squared Error
   */
  def config2MSE(params:Config)
                (implicit ev: TensorNumeric[Float]): MeanSquaredError[Float] = {

    /*
     * Whether losses are averaged over observations
     * for each mini-batch. Default is true.
     *
     * If false, the losses are instead summed for
     * each mini-batch.
     */
    val sizeAverage = getAsBoolean(params, ModelNames.SIZE_AVERAGE, default = true)
    objectives.MeanSquaredError(sizeAverage)

  }
  /*
   * Mean Squared Logarithmic Error
   */
  def config2MSLE(params:Config)
                 (implicit ev: TensorNumeric[Float]): MeanSquaredLogarithmicError[Float] = {
    objectives.MeanSquaredLogarithmicError()
  }
  /*
   * Poisson
   */
  def config2Poisson(params:Config)
                    (implicit ev: TensorNumeric[Float]): Poisson[Float] = {
    objectives.Poisson()
  }
  /*
   * RankHinge
   */
  def config2RankHinge(params:Config)
                      (implicit ev: TensorNumeric[Float]): RankHinge[Float] = {

    /* Margin */
    val margin = getAsDouble(params, ModelNames.MARGIN, 1D)
    objectives.RankHinge(margin = margin)
  }
  /*
   * SparseCategoricalCrossEntropy
   */
  def config2SCCE(params:Config)
                 (implicit ev: TensorNumeric[Float]): SparseCategoricalCrossEntropy[Float] = {

    /* Whether to accept log-probabilities or probabilities */
    val logProbAsInput = getAsBoolean(params, ModelNames.LOG_PROBS_AS_INPUT, default = false)

    /*
     * Whether target labels start from 0. Default is true.
		 * If false, labels start from 1.
		 */
    val zeroBasedLabel = getAsBoolean(params, ModelNames.ZERO_BASED_LABEL, default = true)
    /*
     * Whether losses are averaged over observations
     * for each mini-batch. Default is true.
     *
     * If false, the losses are instead summed for
     * each mini-batch.
     */
    val sizeAverage = getAsBoolean(params, ModelNames.SIZE_AVERAGE, default = true)

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
    val paddingValue = getAsInt(params, ModelNames.PADDING_VALUE, -1)
    objectives.SparseCategoricalCrossEntropy(
        logProbAsInput = logProbAsInput,
        zeroBasedLabel = zeroBasedLabel,
        sizeAverage = sizeAverage,
        paddingValue = paddingValue)
  }
  /*
   * Squared Hinge
   */
  def config2SquaredHinge(params:Config)
                         (implicit ev: TensorNumeric[Float]): SquaredHinge[Float] = {

    /* Margin */
    val margin = getAsDouble(params, ModelNames.MARGIN, 1D)
    /*
     * Whether losses are averaged over observations
     * for each mini-batch. Default is true.
     *
     * If false, the losses are instead summed for
     * each mini-batch.
     */
    val sizeAverage = getAsBoolean(params, ModelNames.SIZE_AVERAGE, default = true)
    objectives.SquaredHinge(margin = margin, sizeAverage = sizeAverage)
  }

}
