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

import com.intel.analytics.zoo.pipeline.api.keras.layers.InputLayer
import com.intel.analytics.zoo.pipeline.api.keras.models.Sequential
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric

import com.typesafe.config.{Config, ConfigObject}

class ModelBuilder(implicit ev: TensorNumeric[Float]) extends LayerBuilder
  with OptimizerBuilder with LossBuilder with MetricsBuilder {
  /**
   * The main method to transform a serialized JSON
   * representation of a Keras like deep learning
   * model into software
   */
  def build(spec:String, folder:String, compile:Boolean = true):Option[Sequential[Float]] = {

    val keras = buildKeras(spec, folder, compile)
    if (keras.isRight) {
      val message = keras.right.get.getLocalizedMessage
      logger.error(message)

      None
    }
    else
      Some(keras.left.get)

  }

  def buildKeras(spec: String, folder:String, compile: Boolean = true): Either[Sequential[Float], Throwable] = {
    try {
      /*
       * STAGE #1: Transform serialized JSON representation
       * of a deep learning model specification, represented
       * as a Typesafe configuration
       */
      val config = spec2Config(spec)
      /*
       * STAGE #2: Transform a Typesafe model specification
       * into an executable model
       */
      val model = config2KerasModel(config, folder, compile)
      Left(model)

    } catch {
      case t: Throwable =>

        val now = System.currentTimeMillis
        val date = new java.util.Date(now)

        val message = s"[ERROR] ${date.toString} - ${t.getLocalizedMessage}"
        Right(new Throwable(message))
    }
  }

  def config2KerasModel(config: Config, folder:String, compile: Boolean): Sequential[Float] = {

    try {

      val conf = config.getConfig(ModelNames.MODEL)
      /*
       * Accepted model templates
       */
      val `type` = conf.getString(ModelNames.TYPE)
      if (`type`.toLowerCase != ModelNames.KERAS) {

        val message = s"The model template '${`type`}' is not supported."
        logger.error(message)

        throw new Exception(message)

      }

      val sequential = Sequential[Float]()

      input2Model(sequential, conf)
      layers2Model(sequential, conf, folder)

      if (!compile) return sequential

      val optimizer = try {
        config2Optimizer(conf.getConfig(ModelNames.OPTIMIZER))

      } catch {
        case _: Throwable =>

          val message = s"The model optimizer is not specified. Cannot compile model."
          logger.warn(message)

          null
      }

      val loss = try {
        config2Loss(conf.getConfig(ModelNames.LOSS))

      } catch {
        case _: Throwable =>

          val message = s"The model loss is not specified. Cannot compile model."
          logger.warn(message)

          null
      }

      val metrics = try {
        config2Metrics(conf.getConfig(ModelNames.METRICS))

      } catch {
        case _: Throwable =>

          val message = s"The model metrics is not specified."
          logger.warn(message)

          null
      }

      if (optimizer == null || loss == null) {

        val message = s"The model cannot be compile. Optimizer and/or loss are not specified."
        logger.error(message)

        throw new Exception(message)
      }

      if (metrics == null) {
        sequential.compile(optimizer = optimizer, loss = loss)

      } else
        sequential.compile(optimizer = optimizer, loss = loss, metrics = metrics)

      sequential

    } catch {
      case t: Throwable =>
        throw new Exception(t.getLocalizedMessage)
    }
  }

  private def input2Model(model: Sequential[Float], conf: Config): Unit = {
    /*
     * Convert `input` specification into Shape first
     * to enable layer building with respect to this input
     */
    val input = try {
      conf.getList(ModelNames.INPUT)

    } catch {
      case _: Throwable =>

        val message = s"The model input is not specified."
        logger.warn(message)

        null
    }

    if (input != null) {

      val inputLayer = InputLayer(inputShape = config2Shape(input))
      model.add(inputLayer)

    }

  }

  private def layers2Model(model: Sequential[Float], conf: Config, folder:String): Unit = {

    val version = getAsString(conf, ModelNames.VERSION, "V1")

    val layers = try {
      conf.getList(ModelNames.LAYERS)

    } catch {
      case _: Throwable =>

        val message = s"The model layers is not specified."
        logger.error(message)

        throw new Exception(message)
    }

    val size = layers.size
    for (i <- 0 until size) {

      val cval = layers.get(i)
      /*
       * Unpack layer as Config
       */
      cval match {
        case configObject: ConfigObject =>

          val layer = configObject.toConfig

          val kerasLayer = config2Layer(layer, version, folder)
          model.add(kerasLayer)

        case _ =>
          throw new Exception(s"Layer $i is not specified as configuration object.")
      }

    }

  }

}
