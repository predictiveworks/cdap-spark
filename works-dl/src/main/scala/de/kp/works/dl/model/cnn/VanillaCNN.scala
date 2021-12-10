package de.kp.works.dl.model.cnn
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
import com.intel.analytics.zoo.pipeline.api.keras.models.Sequential
import de.kp.works.dl.DLForecast
import de.kp.works.dl.model.builder.{Loss, ModelBuilder, Optimizer}

object VanillaCNN {

  private val template = """
    model {
      name    = "VanillaCNN",
      type    = "keras",
      version = "V2",
      input   = [%input1,%input2],
      layers  = [
        {
          name = "layer1",
          type = "Convolution1D",
          params = {
            activation = "%activation1",
            bias = %bias1,
            filters = %filters1,
            kernelSize= %kernelSize1,
            strides = %strides1,
          },
        },
        {
          name = "layer2",
          type = "AveragePooling1D",
          params = {
            poolSize = %poolSize1,
            poolStrides = %poolStrides1,
          },
        },
        {
          name = "layer3",
          type = "Convolution1D",
          params = {
            activation = "%activation2",
            bias = %bias2,
            filters = %filters2,
            kernelSize= %kernelSize2,
            strides = %strides2,
          },
        },
        {
          name = "layer4",
          type = "AveragePooling1D",
          params = {
            poolSize = %poolSize2,
            poolStrides = %poolStrides2,
          },
        },
        {
          name = "layer5",
          type = "Flatten",
          params = {
          },
        },
        {
          name = "layer6",
          type = "Dense",
          params = {
            activation = "%activation3",
            units = %units,
          },
        },

      ],
      optimizer = {
          name = "optimizer",
          type = "%optimizer",
          params = {
            learningRate = %learningRate,
          },
      },
      loss = {
          name = "loss",
          type = "%loss",
          params = {
          },
      }
    }
  """

  def apply(folder:String, settings:Map[String,String]):Sequential[Float] = {

    val input1 = settings.getOrElse("input1", "5")
    val input2 = settings.getOrElse("input2", "1")
    /*
     * First convolution layer
     */
    val activation1 = settings.getOrElse("activation1", "relu")
    val bias1       = settings.getOrElse("bias1", "true")
    val filters1    = settings.getOrElse("filters1", "128")
    val kernelSize1 = settings.getOrElse("kernelSize1", "2")
    val strides1    = settings.getOrElse("strides1", "1")
    /*
     * First average pooling layer
     */
    val poolSize1    = settings.getOrElse("poolSize1", "2")
    val poolStrides1 = settings.getOrElse("poolStrides1", "1")
    /*
     * Second convolution layer
     */
    val activation2 = settings.getOrElse("activation2", "relu")
    val bias2       = settings.getOrElse("bias2", "true")
    val filters2    = settings.getOrElse("filters2", "64")
    val kernelSize2 = settings.getOrElse("kernelSize2", "2")
    val strides2    = settings.getOrElse("strides2", "1")
    /*
     * Second average pooling layer
     */
    val poolSize2    = settings.getOrElse("poolSize2", "2")
    val poolStrides2 = settings.getOrElse("poolStrides2", "1")
    /*
     * Dense layer
     */
    val activation3 = settings.getOrElse("activation3", "linear")
    val units       = settings.getOrElse("units", "1")

    /*
     * Check whether provided `optimizer` is supported
     */
    val optimizer = settings.getOrElse("optimizer", "Adam")

    if (!Optimizer.getOptimizers.contains(optimizer)) {
      throw new Exception(s"The optimizer '$optimizer' is not supported.")
    }

    val learningRate = settings.getOrElse("learningRate", "0.001")

    /*
     * Check whether provided `loss` is supported
     */
    val loss = settings.getOrElse("loss", "MSE")

    if (!Loss.getLosses.contains(loss)) {
      throw new Exception(s"The loss '$loss' is not supported.")
    }

    /*
     * Retrieve model specification from template
     */
    val spec = template
      .replace("%input1", input1)
      .replace("%input2", input2)

      /* Convolution 1D #1 */
      .replace("%activation1", activation1)
      .replace("%bias1",       bias1)
      .replace("%filters1",    filters1)
      .replace("%kernelSize1", kernelSize1)
      .replace("%strides1",    strides1)

      /* Average Pooling 1D #1 */
      .replace("%poolSize1",    poolSize1)
      .replace("%poolStrides1", poolStrides1)

      /* Convolution 1D #2 */
      .replace("%activation2", activation2)
      .replace("%bias2",       bias2)
      .replace("%filters2",    filters2)
      .replace("%kernelSize2", kernelSize2)
      .replace("%strides2",    strides2)

      /* Average Pooling 1D #2 */
      .replace("%poolSize2",    poolSize2)
      .replace("%poolStrides2", poolStrides2)

      /* Dense */
      .replace("%activation3", activation3)
      .replace("%units",       units)

      /* Compile */
      .replace("%optimizer",    optimizer)
      .replace("%learningRate", learningRate)
      .replace("%loss", loss)

    val builder = new ModelBuilder()
    val result = builder.buildKeras(spec, folder)

    if (result.isRight)
      throw new Exception(result.right.get.getLocalizedMessage)

    val model = result.left.get
    model

  }

}

class VanillaCNN(
  /*
   * The list of activations to be mapped onto the respective
   * Keras layer in the provided order.
   */
  activations:Array[String] = Array("relu", "relu", "linear"),
  /*
   * The list of biases to be mapped onto the respective
   * Keras layer in the provided order.
   */
  biases:Array[Boolean] = Array(true, true),
  /*
   * The list of filters to be mapped onto the respective
   * Keras layer in the provided order.
   */
  filters:Array[Int] = Array(128, 64), // 128,64
  /*
   * The (distributed) file system folder that contains
   * pre-trained deep learning models
   */
  folder:String = "",
  /*
   * Dimension of the input features and the dimension of the
   * input label. In an univariate time series, the number of
   * features is equal to the chosen `time lag`.
   */
  input:Array[Int] = Array(5,1),
  /*
   * The list of kernelSizes to be mapped onto the respective
   * Keras layer in the provided order.
   */
  kernelSizes:Array[Int] = Array(2, 2),
  /*
   * The learning rate. The default value is 0.001.
   */
  learningRate:Double=0.001,
  /*
   * The target function to optimize on. The default value is MSE.
   */
  loss:String = "MSE",
  /*
   * The optimizer used for training. The default value is Adam.
   */
  optimizer:String = "Adam",
  /*
   * The list of poolSizes to be mapped onto the respective
   * Keras layer in the provided order.
   */
  poolSizes:Array[Int] = Array(2, 2),
  /*
   * The list of poolStrides to be mapped onto the respective
   * Keras layer in the provided order.
   */
  poolStrides:Array[Int] = Array(1, 1),
  /*
   * The list of strides to be mapped onto the respective
   * Keras layer in the provided order.
   */
  strides:Array[Int] = Array(1, 1),
  /*
   * The output dimension
   */
  units:Int = 1) extends DLForecast {

  /*
   * A kernel initializer of `variance_scaling` is not supported
   * and must be omitted with this neural network.
   */
  private val params = Map(
    "activation1"  -> activations(0),
    "activation2"  -> activations(1),
    "activation3"  -> activations(2),
    "bias1"        -> biases(0).toString,
    "bias2"        -> biases(1).toString,
    "filters1"     -> filters(0).toString,
    "filters2"     -> filters(1).toString,
    "input1"       -> input(0).toString,
    "input2"       -> input(1).toString,
    "kernelSize1"  -> kernelSizes(0).toString,
    "kernelSize2"  -> kernelSizes(1).toString,
    "learningRate" -> learningRate.toString,
    "loss"         -> loss,
    "optimizer"    -> optimizer,
    "poolSize1"    -> poolSizes(0).toString,
    "poolSize2"    -> poolSizes(1).toString,
    "poolStrides1" -> poolStrides(0).toString,
    "poolStrides2" -> poolStrides(1).toString,
    "strides1"     -> strides(0).toString,
    "strides2"     -> strides(1).toString,
    "units"        -> units.toString)

  def getModel: Sequential[Float] = VanillaCNN(folder, params)

}