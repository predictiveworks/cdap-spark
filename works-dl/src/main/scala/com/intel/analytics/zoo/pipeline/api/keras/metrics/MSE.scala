package com.intel.analytics.zoo.pipeline.api.keras.metrics
/*
 * Copyright 2018 Analytics Zoo Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.intel.analytics.bigdl.nn.MSECriterion
import com.intel.analytics.bigdl.nn.abstractnn.Activity
import com.intel.analytics.bigdl.optim.{LossResult, ValidationMethod}
import com.intel.analytics.bigdl.tensor.Tensor
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric

import scala.reflect.ClassTag

/**
 * __KUP__
 *
 * This metrics does not exist in version [0.7.0] and
 * is a copy of the respective [0.10.0] version.
 */
class MSE[T: ClassTag](implicit ev: TensorNumeric[T])
  extends ValidationMethod[T] {
  override def apply(output: Activity, target: Activity): LossResult = {
    val _output = output.asInstanceOf[Tensor[T]]
    val _target = target.asInstanceOf[Tensor[T]]
    val loss = ev.toType[Float](MSECriterion().forward(_output, _target))
    val count = 1

    new LossResult(loss, count)
  }
  override def format(): String = "MSE"
}