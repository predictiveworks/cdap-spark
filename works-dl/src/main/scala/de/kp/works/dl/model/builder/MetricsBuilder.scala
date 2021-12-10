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

import com.intel.analytics.bigdl.optim.ValidationMethod
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric
import com.intel.analytics.zoo.pipeline.api.keras.metrics._
import com.typesafe.config.Config
import de.kp.works.dl.model.ModelNames

trait MetricsBuilder extends SpecBuilder {

  val DL_ACCURACY = "Accuracy"
  val DL_BINARY_ACCURACY = "BinaryAccuracy"
  val DL_CATEGORICAL_ACCURACY = "CategoricalAccuracy"
  val DL_MAE = "MAE"
  val DL_MSE = "MSE"
  val DL_SPARSE_CATEGORICAL_ACCURACY = "SparseCategoricalAccuracy"
  val DL_TOP5_ACCURACY = "Top5Accuracy"

  def getMetrics = List(
    DL_ACCURACY,
    DL_BINARY_ACCURACY,
    DL_CATEGORICAL_ACCURACY,
    DL_MAE,
    DL_MSE,
    DL_SPARSE_CATEGORICAL_ACCURACY,
    DL_TOP5_ACCURACY)

  def config2Metrics(metrics: Config)
                    (implicit ev: TensorNumeric[Float]): List[ValidationMethod[Float]] = {

    val params = metrics.getConfig(ModelNames.PARAMS)

    metrics.getString(ModelNames.TYPE) match {
      case DL_ACCURACY =>

        /* This metrics is deprecated */
        logger.warn("Metrics 'Accuracy' is deprecated. Use 'SparseCategoricalAccuracy', "
          + "'CategoricalAccuracy' or 'BinaryAccuracy' instead")

        /*
         * Whether target labels start from 0. Default is true.
         * If false, labels start from 1. Note that this only
         * takes effect for multi-class classification.
         *
         * For binary classification, labels ought to be 0 or 1.
         */
        val zeroBasedLabel = getAsBoolean(params, ModelNames.ZERO_BASED_LABEL, default = true)
        List(new Accuracy(zeroBasedLabel = zeroBasedLabel))

      case DL_BINARY_ACCURACY =>
        /*
         * Measures top1 accuracy for binary classification
         * with zero-base index.
         */
        List(new BinaryAccuracy())

      case DL_CATEGORICAL_ACCURACY =>
        /*
         * Measures top1 accuracy for multi-class with "one-hot"
         * target.
         */
        List(new CategoricalAccuracy())

      case DL_MAE => List(new MAE[Float]())
      case DL_MSE => List(new MSE[Float]())

      case DL_SPARSE_CATEGORICAL_ACCURACY =>
        /*
         * Measures top1 accuracy for multi-class classification
         * with sparse target and zero-base index.
         */
        List(new SparseCategoricalAccuracy())

      case DL_TOP5_ACCURACY =>
        /*
         * Measures top5 accuracy for multi-class classification.
         *
        *
         * Whether target labels start from 0. Default is true.
         * If false, labels start from 1. Note that this only
         * takes effect for multi-class classification.
         *
         * For binary classification, labels ought to be 0 or 1.
         */
        val zeroBasedLabel = getAsBoolean(params, ModelNames.ZERO_BASED_LABEL, default = true)
        List(new Top5Accuracy(zeroBasedLabel = zeroBasedLabel))

      case _ => null
    }
  }
}
