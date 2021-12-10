package de.kp.works.dl.model.builder
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

import com.intel.analytics.bigdl.optim
import com.intel.analytics.bigdl.optim._
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric
import com.typesafe.config.Config
import de.kp.works.dl.model.ModelNames

object Optimizer {

  def getOptimizers = List(
    "Adadelta",
    "Adagrad",
    "Adam",
    "Adamax",
    "RMSprop",
    "SGD")

}

trait OptimizerBuilder extends SpecBuilder {

  def config2Optimizer(optimizer: Config): OptimMethod[Float] = {

    val params = optimizer.getConfig(ModelNames.PARAMS)
    optimizer.getString(ModelNames.TYPE) match {
      case "Adadelta" =>
        config2Adadelta(params)
      case "Adagrad" =>
        config2Adagrad(params)
      case "Adam" =>
        config2Adam(params)
      case "Adamax" =>
        config2Adamax(params)
      case "RMSprop" =>
        config2RMSprop(params)
      case "SGD" =>
        config2SGD(params)
      case _ => null
    }

  }

  def config2Regularizer(optimizer: Config)
                        (implicit ev: TensorNumeric[Float]): L1L2Regularizer[Float] = {

    val params = optimizer.getConfig(ModelNames.PARAMS)
    optimizer.getString(ModelNames.TYPE) match {
      case "L1" => config2L1(params)
      case "L2" => config2L2(params)
      case _ => null
    }

  }

  def config2L1(params: Config)
               (implicit ev: TensorNumeric[Float]): L1Regularizer[Float] = {

    /* l1 regularization or learning rate */
    val learningRate = params.getDouble(ModelNames.LEARNING_RATE)

    optim.L1Regularizer(l1 = learningRate)

  }

  def config2L2(params: Config)
               (implicit ev: TensorNumeric[Float]): L2Regularizer[Float] = {

    /* l2 regularization or learning rate */
    val learningRate = params.getDouble(ModelNames.LEARNING_RATE)

    optim.L2Regularizer(l2 = learningRate)

  }

  def config2Adadelta(params: Config): Adadelta[Float] = {

    /* Decay rate */
    val decayRate = getAsDouble(params, ModelNames.DECAY_RATE, 0.9)

    /* Epsilon for numerical stability */
    val epsilon = getAsDouble(params, ModelNames.EPSILON, 1e-10)

    new optim.Adadelta(decayRate = decayRate, Epsilon = epsilon)

  }

  def config2Adagrad(params: Config): Adagrad[Float] = {

    /* Learning rate */
    val learningRate = getAsDouble(params, ModelNames.LEARNING_RATE, 1e-3)

    /* Decay rate */
    val decayRate = getAsDouble(params, ModelNames.DECAY_RATE, 0D)

    /* Weight decay */
    val weightDecay = getAsDouble(params, ModelNames.WEIGHT_DECAY, 0D)

    new optim.Adagrad(learningRate = learningRate, learningRateDecay = decayRate, weightDecay = weightDecay)

  }

  def config2Adam(params: Config): Adam[Float] = {

    /* Learning rate */
    val learningRate = getAsDouble(params, ModelNames.LEARNING_RATE, 1e-3)

    /* Decay rate */
    val decayRate = getAsDouble(params, ModelNames.DECAY_RATE, 0D)

    /* First moment coefficient */
    val beta1 = getAsDouble(params, ModelNames.BETA1, 0.9)

    /* Second moment coefficient */
    val beta2 = getAsDouble(params, ModelNames.BETA2, 0.999)

    /* Epsilon for numerical stability */
    val epsilon = getAsDouble(params, ModelNames.EPSILON, 1e-8)

    new optim.Adam(learningRate = learningRate, learningRateDecay = decayRate, beta1 = beta1, beta2 = beta2, Epsilon = epsilon)

  }


  def config2Adamax(params: Config): Adamax[Float] = {

    /* Learning rate */
    val learningRate = getAsDouble(params, ModelNames.LEARNING_RATE, 0.002)

    /* First moment coefficient */
    val beta1 = getAsDouble(params, ModelNames.BETA1, 0.9)

    /* Second moment coefficient */
    val beta2 = getAsDouble(params, ModelNames.BETA2, 0.999)

    /* Epsilon for numerical stability */
    val epsilon = getAsDouble(params, ModelNames.EPSILON, 1e-38)

    new optim.Adamax(learningRate = learningRate, beta1 = beta1, beta2 = beta2, Epsilon = epsilon)

  }

  def config2RMSprop(params: Config): RMSprop[Float] = {

    /* Learning rate */
    val learningRate = getAsDouble(params, ModelNames.LEARNING_RATE, 1e-2)

    /* Decay rate */
    val decayRate = getAsDouble(params, ModelNames.DECAY_RATE, 0D)

    /* Epsilon for numerical stability */
    val epsilon = getAsDouble(params, ModelNames.EPSILON, 1e-8)

    new optim.RMSprop(learningRate = learningRate, decayRate = decayRate, Epsilon = epsilon)

  }

  def config2SGD(params: Config): SGD[Float] = {

    /* Learning rate */
    val learningRate = getAsDouble(params, ModelNames.LEARNING_RATE, 1e-3)

    /* Decay rate */
    val decayRate = getAsDouble(params, ModelNames.DECAY_RATE, 0D)

    /* Weight decay */
    val weightDecay = getAsDouble(params, ModelNames.WEIGHT_DECAY, 0D)

    /* Momentum */
    val momentum = getAsDouble(params, ModelNames.MOMENTUM, 0D)

    /* Damping for momentum */
    val dampening = getAsDouble(params, ModelNames.DAMPENING, Double.MaxValue)

    /* Enables Nesterov momentum */
    val nesterov = getAsBoolean(params, ModelNames.NESTEROV, default = false)

    /*
     * The following parameters are actually not supported:
     *
     * - learningRateSchedule
     * - learningRates
     * - weightDecays
     */
    new optim.SGD(
      learningRate = learningRate,
      learningRateDecay = decayRate,
      weightDecay = weightDecay,
      momentum = momentum,
      dampening = dampening,
      nesterov = nesterov)
  }

}
