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

import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric
import com.intel.analytics.zoo.pipeline.api.Net
import com.intel.analytics.zoo.pipeline.api.keras.models.KerasNet
import com.intel.analytics.zoo.pipeline.api.net.{GraphNet, TFNet, TorchNet}

class LoaderUtils(implicit val ev: TensorNumeric[Float]) {
  
  def load(
      modelType:String, modelPath:String, 
      defPath:Option[String] = None, weightPath:Option[String] = None):Either[KerasNet[Float], GraphNet[Float]] = {

    modelType match {
      case "analytics_zoo" =>
        /*
         * Load an existing model defined using the Analytics Zoo Keras-style API.
         * The model path and optionally weight path if exists must be specified,
         * where the model was saved.
         */
        val model:KerasNet[Float] = if (weightPath.isDefined) {
          Net.load(modelPath, weightPath.get)

        } else {
          Net.load(modelPath)
        }

        Left(model)

      case "big_dl" =>
        val model:GraphNet[Float] = if (weightPath.isDefined) {
          Net.loadBigDL(modelPath, weightPath.get)

        } else {
          Net.loadBigDL(modelPath)
        }

        Right(model)
      case "caffe" =>
        val model:GraphNet[Float] = Net.loadCaffe(defPath.get, modelPath)
        Right(model)

      case "torch" =>
        val model:GraphNet[Float] = Net.loadTorch(modelPath)
        Right(model)
    }
  }
  /*
   * NOTE: As an alternative one may use the `inference` interface
   */
  def loadTF(modelPath:String):TFNet = {
    /*
     * TFNet can only be used for model inference or as a feature
     * extractor for fine tuning a model. When used as feature extractor, 
     * there should not be any trainable layers before TFNet, as all the 
     * gradient from TFNet is set to zero.
     */
    val config = TFNet.defaultSessionConfig
    val model = TFNet(modelPath, config)
    
    model
  }
  
  def loadPyTorch(modelPath:String):TorchNet = {
    val model = TorchNet(modelPath)
    model
  }
  
}