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

import com.intel.analytics.bigdl.nn.abstractnn.Activity
import com.intel.analytics.bigdl.nn.keras.KerasLayer
import com.intel.analytics.bigdl.optim.L1L2Regularizer
import com.intel.analytics.bigdl.tensor.Tensor
import com.intel.analytics.bigdl.tensor.TensorNumericMath._
import com.intel.analytics.bigdl.utils.Shape
import com.intel.analytics.zoo.pipeline.api.keras.layers._
import com.intel.analytics.zoo.pipeline.api.keras2.layers
import com.intel.analytics.zoo.pipeline.api.keras2.layers.{Conv1D, Conv2D}
import com.intel.analytics.zoo.pipeline.api.net.GraphNet
import com.intel.analytics.zoo.pipeline.api.{keras, keras2}
import com.typesafe.config.{Config, ConfigList}

trait LayerBuilder extends SpecBuilder with OptimizerBuilder {
  /*
   * The list of supported model types for pre-trained models
   */
  private val modelTypes = Array("analytics_zoo", "big_dl", "caffe", "torch")

  def config2Layer(config: Config, kerasVersion: String, modelFolder:String)
                  (implicit ev:TensorNumeric[_]) = {

    val bidirectional = getAsBoolean(config, "biDirectional", default = false)
    val timeDistributed = getAsBoolean(config, "timeDistributed", default = false)

    val kerasLayer = config.getString(ModelNames.TYPE) match {

      /* V1 & V2 */
      case "AveragePooling1D" =>
        if (kerasVersion == "V1")
          config2AvgPool1D_V1(config)

        else
          config2AvgPool1D_V2(config)

      case "BatchNormalization" =>
        config2BatchNorm(config)

      /* V1 & V2 */
      case "Convolution1D" =>
        if (kerasVersion == "V1")
          config2Conv1D_V1(config)

        else
          config2Conv1D_V2(config)

      /* V1 & V2 */
      case "Convolution2D" =>
        if (kerasVersion == "V1")
          config2Conv2D_V1(config)

        else
          config2Conv2D_V2(config)

      case "ConvLSTM2D" =>
        config2ConvLSTM2D(config)

      /* V1 & V2 */
      case "Dense" =>
        if (kerasVersion == "V1") {
          val dense = config2Dense_V1(config)
          if (timeDistributed)
            keras.layers.TimeDistributed(dense.asInstanceOf[KerasLayer[Activity, Tensor[Float], Float]])
          else
            dense
        }
        else {
          val dense = config2Dense_V2(config)
          if (timeDistributed)
            keras.layers.TimeDistributed(dense.asInstanceOf[KerasLayer[Activity, Tensor[Float], Float]])
          else
            dense
        }

      /* V1 & V2 */
      case "Dropout" =>
        if (kerasVersion == "V1")
          config2Dropout_V1(config)

        else
          config2Dropout_V2(config)

      case "ELU" =>
        config2ELU(config)

      /* V1 & V2 */
      case "Flatten" =>
        if (kerasVersion == "V1")
          config2Flatten_V1()

        else
          config2Flatten_V2(config)

      /* V1 & V2 */
      case "GlobalAveragePooling1D" =>
        if (kerasVersion == "V1")
          config2GlobalAvgPool1D_V1(config)

        else
          config2GlobalAvgPool1D_V2(config)

      /* V1 & V2 */
      case "GlobalMaxPooling1D" =>
        if (kerasVersion == "V1")
          config2GlobalMaxPool1D_V1(config)

        else
          config2GlobalMaxPool1D_V2(config)

      case "GRU" =>
        config2GRU(config)

      case "LSTM" =>
        val lstm = config2LSTM(config)
        if (bidirectional)
        /*
           * The current implementation supports the
           * default `mergeMode` = `concat`.
           */
          keras.layers.Bidirectional(lstm)

        else
          lstm

      /* V1 & V2 */
      case "MaxPooling1D" =>
        if (kerasVersion == "V1")
          config2MaxPooling1D_V1(config)

        else
          config2MaxPooling1D_V2(config)

      case "MaxPooling2D" =>
        config2MaxPooling2D(config)

      case "Pretrained" =>
        config2Pretrained(config, modelFolder)

      case "RepeatVector" =>
        config2RepeatVector(config)
    }

    val name = config.getString(ModelNames.NAME)
    kerasLayer.setName(name)

  }

  /** *** PRETRAINED **** */

  def config2Pretrained(layer: Config, modelFolder:String): KerasLayer[Activity, Activity, Float] = {

    val params = layer.getConfig(ModelNames.PARAMS)

    val modelType = getStringParam(params, ModelNames.MODEL_TYPE).get
    if (!modelTypes.contains(modelType)) {

      val message = s"The model type '$modelType' is not supported."
      logger.error(message)

      throw new Exception(message)

    }

    val modelPath = getModelPath(params, modelFolder)

    val defPath = getStringParam(params, ModelNames.DEF_PATH)
    val weightPath = getStringParam(params, ModelNames.WEIGHT_PATH)

    val loader = new LoaderUtils()
    try {
      modelType match {
        case "analytics_zoo" =>

          val model = loader.load(modelType = modelType, modelPath = modelPath, weightPath = weightPath)
          //val kerasModel = truncateKerasNet(baseConf, model.left.get)
          throw new Exception("Not supported yet")

        case "big_dl" =>

          val model = loader.load(modelType = modelType, modelPath = modelPath, weightPath = weightPath)

          val truncNet = truncateGraphNet(model.right.get, layer)
          val freezeNet = freezeGraphNet(truncNet, layer)

          freezeNet.toKeras()

        case "caffe" =>

          val model = loader.load(modelType = modelType, modelPath = modelPath, defPath = defPath)

          val truncNet = truncateGraphNet(model.right.get, layer)
          val freezeNet = freezeGraphNet(truncNet, layer)

          freezeNet.toKeras()
        case "torch" =>

          val model = loader.load(modelType = modelType, modelPath = modelPath)

          val truncNet = truncateGraphNet(model.right.get, layer)
          val freezeNet = freezeGraphNet(truncNet, layer)

          freezeNet.toKeras()

        case _ => throw new Exception(s"Model type '$modelType' is not supported.")
      }

    } catch {
      case t: Throwable =>
        val message = s"Loading pre-trained model failed with: ${t.getLocalizedMessage}"
        throw new Exception(message)
    }

  }

  def getModelPath(params: Config, modelFolder:String): String = {

    val modelPath = getStringParam(params, ModelNames.MODEL_PATH)
    if (modelPath.isDefined) return modelPath.get

    /*
     * Retrieve model path from name and version
     */
    val modelName = getStringParam(params, ModelNames.MODEL_NAME)
    val modelVersion = getStringParam(params, ModelNames.MODEL_VERSION)

    if (modelName.isEmpty || modelVersion.isEmpty)
      throw new Exception(s"Model specification does not provided enough parameters to determine model location.")

    val name = modelName.get
    val version = modelVersion.get

    val fileName = s"analytics-zoo_${name}_imagenet_$version.model"
    s"$modelFolder/$fileName"

  }

  /*
   * Freeze means that the respective layers will not be trained
   */
  def freezeGraphNet(graphNet: GraphNet[Float], layer: Config): GraphNet[Float] = {

    val freeze = getFreeze(layer)
    val newGraphNet = if (freeze != null) {

      val layers = graphNet.getSubModules().map(_.getName)
      if (layers.contains(freeze)) {

        val freezeNet = graphNet.freezeUpTo(freeze)
        /*
         * 'freeze' does not mean that the respective modules
         * are not trainable; therefore the layers are set to
         * training = false as well
         */
        val upTo = layers.zipWithIndex.filter { case (name, _) => name == freeze }.head._2
        freezeNet.getSubModules().zipWithIndex.foreach { case (module, index) =>
          if (index <= upTo) module.evaluate
        }

        freezeNet

      } else
        throw new Exception(s"The pre-trained model does not contain '$freeze'.")

    } else graphNet


    newGraphNet

  }

  def truncateGraphNet(graphNet: GraphNet[Float], layer: Config): GraphNet[Float] = {

    /*
     * Evaluate whether the base model must be truncated,
     * i.e. a certain layer must be defined as output
     */
    val output = getOutput(layer)
    val newGraphNet = if (output != null) {

      val layers = graphNet.getSubModules().map(_.getName)
      if (layers.contains(output)) {
        graphNet.newGraph(output)

      } else
        throw new Exception(s"The pre-trained model does not contain '$output'.")

    } else graphNet


    newGraphNet

  }

  def getFreeze(layer: Config): String = {

    try {
      layer.getString(ModelNames.FREEZE)

    } catch {
      case _: Throwable => null
    }

  }

  def getOutput(layer: Config): String = {

    try {
      layer.getString(ModelNames.OUTPUT)

    } catch {
      case _: Throwable => null
    }

  }

  def getStringParam(params: Config, name: String): Option[String] = {

    try {

      val param = params.getString(name)
      if (param.isEmpty) None else Option(param)

    } catch {
      case _: Throwable => None
    }

  }

  /** *** BASE LAYER **** */

  /*
   * Average Pooling 1D layer
   *
   * Applies average pooling operation for temporal data.
   */
  def config2AvgPool1D_V1(layer: Config): AveragePooling1D[Float] = {

    val params = layer.getConfig(ModelNames.PARAMS)
    /*
     * Size of the region to which average pooling is applied.
     * Default is 2.
     */
    val poolLength = getAsInt(params, ModelNames.POOL_LENGTH, 2)
    /*
     * Factor by which to downscale. Positive integer, or -1.
     *
     * 2 will halve the input. If -1, it will default to poolLength.
     * Default is -1, and in this case it will be equal to poolSize.
     */
    val stride = getAsInt(params, ModelNames.STRIDE, -1)
    /*
     * `borderMode` is either `valid` (default) or `same`.
     */
    val borderMode = getAsString(params, ModelNames.BORDER_MODE, "valid")

    keras.layers.AveragePooling1D(
      poolLength = poolLength,
      stride = stride,
      borderMode = borderMode)

  }

  def config2AvgPool1D_V2(layer: Config): layers.AveragePooling1D[Float] = {

    val params = layer.getConfig(ModelNames.PARAMS)
    /*
     * Size of the region to which average pooling is applied.
     * Default is 2.
     */
    val poolSize = getAsInt(params, ModelNames.POOL_SIZE, 2)
    /*
     * Factor by which to downscale. Positive integer, or -1.
     *
     * 2 will halve the input. If -1, it will default to poolLength.
     * Default is -1, and in this case it will be equal to poolSize.
     */
    val strides = getAsInt(params, ModelNames.STRIDES, -1)
    /*
     * `padding` is either `valid` (default) or `same`.
     */
    val padding = getAsString(params, ModelNames.PADDING, "valid")

    keras2.layers.AveragePooling1D(
      poolSize = poolSize,
      strides = strides,
      padding = padding)

  }

  /*
   * Batch normalization layer
   *
 	 * Normalize the activations of the previous layer at each batch,
   * i.e. applies a transformation that maintains the mean activation
   * close to 0 and the activation standard deviation close to 1.
   *
   * It is a feature-wise normalization, each feature map in the input
   * will be normalized separately.
   *
   * The input of this layer should be 4D.
 	 *
   * When you use this layer as the first layer of a model, you need to
   * provide the argument inputShape (a Single Shape, does not include
   * the batch dimension).
   */
  def config2BatchNorm(layer: Config): BatchNormalization[Float] = {
    /*
     * The current implementation does not support
     * any parameters
     */
    keras.layers.BatchNormalization()
  }

  def config2Conv1D_V1(layer: Config): Convolution1D[Float] = {

    val params = layer.getConfig(ModelNames.PARAMS)

    /* Number of convolution filters to use. */
    val nbFilter: Int = params.getInt(ModelNames.NB_FILTER)

    /* The extension (spatial or temporal) of each filter. */
    val filterLength: Int = params.getInt(ModelNames.FILTER_LENGTH)

    /* Activation function to use. */
    val activation: String = params.getString(ModelNames.ACTIVATION)

    /* Either 'valid' or 'same'. Default is 'valid'. */
    val borderMode = getAsString(params, ModelNames.BORDER_MODE, "valid")

    /* Factor by which to subsample output. Integer. Default is 1.
     *
     * This parameter is also called `strides`.
     */
    val subsampleLength = getAsInt(params, ModelNames.SUBSAMPLE_LENGTH, 1)

    /*
     * Weight regularizer
     *
     * An instance of [[Regularizer]], (eg. L1 or L2 regularization),
     * applied to the input weights matrices. Default is null.
     *
     * NOTE: This is the kernel regularizer in Keras2
     */
    val wRegularizer = params2RegularizerW(params)
    /*
     * Bias regularizer
     *
     * An instance of [[Regularizer]], (eg. L1 or L2 regularization),
     * applied to the bias. Default is null.
     */
    val bRegularizer = params2RegularizerB(params)
    /*
     * Whether to include a bias (i.e. make the layer affine rather
     * than linear). Default is true.
     */
    val bias = getAsBoolean(params, ModelNames.BIAS, default = true)

    keras.layers.Convolution1D(
      nbFilter = nbFilter,
      filterLength = filterLength,
      activation = activation,
      borderMode = borderMode,
      subsampleLength = subsampleLength,
      wRegularizer = wRegularizer,
      bRegularizer = bRegularizer,
      bias = bias)

  }

  def config2Conv1D_V2(layer: Config): Conv1D[Float] = {

    val params = layer.getConfig(ModelNames.PARAMS)
    /*
     * The dimensionality of the output space
     */
    val filters = params.getInt(ModelNames.FILTERS)
    /*
     * An integer specifying the length of the 1D convolution
     * window
     */
    val kernelSize = params.getInt(ModelNames.KERNEL_SIZE)
    /*
     * An integer specifying the stride length of the convolution.
     * Specifying any stride value != 1 is incompatible with specifying
     * any `dilation_rate` value != 1.
     */
    val strides = params.getInt(ModelNames.STRIDES)

    /* Activation function to use. Default is null. You can also pass in
     * corresponding string representations such as 'relu' or 'sigmoid',
     * etc. for simple activations in the factory method.
     */
    val activation: String = params.getString(ModelNames.ACTIVATION)
    /*
     * Whether to include a bias (i.e. make the layer affine rather
     * than linear). Default is true.
     */
    val bias = getAsBoolean(params, ModelNames.BIAS, default = true)
    /*
     * Initializer for the `kernel` weights matrix.
     */
    val kernelInitializer = getAsString(params, ModelNames.KERNEL_INITIALIZER, "glorot_uniform")
    /*
     * Initializer for the bias vector.
     */
    val biasInitializer = getAsString(params, ModelNames.BIAS_INITIALIZER, "zero")
    /*
     * Weight regularizer
     *
     * An instance of [[Regularizer]], (eg. L1 or L2 regularization),
     * applied to the input weights matrices. Default is null.
     *
     * NOTE: This is the kernel regularizer in Keras2
     */
    val kernelRegularizer = params2RegularizerW(params)
    /*
     * Bias regularizer
     *
     * An instance of [[Regularizer]], (eg. L1 or L2 regularization),
     * applied to the bias. Default is null.
     */
    val biasRegularizer = params2RegularizerB(params)

    /*
     * Regularizer function applied to the bias vector. Default is null.
     */
    keras2.layers.Conv1D(
      filters = filters,
      kernelSize = kernelSize,
      strides = strides,
      /*
       * `padding` is a value of `valid`, `causal` or `same`. `valid` means
       * no padding (default), while `same` results in padding the input such
       * that output has the same length as the original input.
       *
       * `causual` results in causal (dilated) convolutions, e.g. output[t]
       * does not depend on input[t+1:]. Useful when modeling temporal data
       * where the model should not violate the temporal order.
       */
      padding = "valid",
      activation = activation,
      useBias = bias,
      kernelInitializer = kernelInitializer,
      biasInitializer = biasInitializer,
      kernelRegularizer = kernelRegularizer,
      biasRegularizer = biasRegularizer)

  }

  def config2Conv2D_V1(layer: Config): Convolution2D[Float] = {

    val params = layer.getConfig(ModelNames.PARAMS)

    /* Number of convolution filters to use. */
    val nbFilter: Int = params.getInt(ModelNames.NB_FILTER)

    /* Number of rows in the convolution kernel. */
    val nbRow: Int = params.getInt(ModelNames.NB_ROW)

    /*  Number of columns in the convolution kernel. */
    val nbCol: Int = params.getInt(ModelNames.NB_COL)

    /* Activation function to use. */
    val activation: String = params.getString(ModelNames.ACTIVATION)

    /* Either 'valid' or 'same'. Default is 'valid'. */
    val borderMode = getAsString(params, ModelNames.BORDER_MODE, "valid")

    /*
     * Int array of length 2 corresponding to the step of
     * the convolution in the height and width dimension.
     * Also called strides elsewhere. Default is (1, 1).
     */
    val subsample = try {

      val array = getAsIntArray(params.getList(ModelNames.SUBSAMPLE))
      (array(0), array(1))

    } catch {
      case _: Throwable => (1, 1)
    }
    /*
     * Format of input data. Either DataFormat.NCHW (dimOrdering='th') or
 		 * DataFormat.NHWC (dimOrdering='tf'). Default is NCHW.
     */
    val dimOrdering = getAsString(params, ModelNames.DIM_ORDERING, "th")

    /*
     * Weight regularizer
     *
     * An instance of [[Regularizer]], (eg. L1 or L2 regularization),
     * applied to the input weights matrices. Default is null.
     */
    val wRegularizer = params2RegularizerW(params)
    /*
     * Bias regularizer
     *
     * An instance of [[Regularizer]], (eg. L1 or L2 regularization),
     * applied to the bias. Default is null.
     */
    val bRegularizer = params2RegularizerB(params)
    /*
     * Whether to include a bias (i.e. make the layer affine rather
     * than linear). Default is true.
     */
    val bias = getAsBoolean(params, ModelNames.BIAS, default = true)

    keras.layers.Convolution2D(
      nbFilter = nbFilter,
      nbRow = nbRow,
      nbCol = nbCol,
      activation = activation,
      borderMode = borderMode,
      subsample = subsample,
      dimOrdering = dimOrdering,
      wRegularizer = wRegularizer,
      bRegularizer = bRegularizer,
      bias = bias)

  }

  /**
   * 2D convolution layer (e.g. spatial convolution over images).
   *
   * This layer creates a convolution kernel that is convolved
   * with the layer input to produce a tensor of outputs.
   *
   * If `use_bias` is True, a bias vector is created and added
   * to the outputs. Finally, if `activation` is not `None`, it
   * is applied to the outputs as well.
   *
   * Input shape
   *
   * 4D tensor with shape:
   * `(samples, channels, rows, cols)` if data_format='channels_first'
   *
   * or 4D tensor with shape:
   * `(samples, rows, cols, channels)` if data_format='channels_last'.
   *
   * Output shape
   *
   * 4D tensor with shape:
   * `(samples, filters, new_rows, new_cols)` if data_format='channels_first'
   *
   * or 4D tensor with shape:
   * `(samples, new_rows, new_cols, filters)` if data_format='channels_last'.
   * `rows` and `cols` values might have changed due to padding.
   */
  def config2Conv2D_V2(layer: Config): Conv2D[Float] = {

    val params = layer.getConfig(ModelNames.PARAMS)
    /*
     * The dimensionality of the output space, i.e. the number of
     * convolution filters to use.
     */
    val filters: Int = params.getInt(ModelNames.FILTERS)
    /*
     * An integer or tuple/list of a single integer, specifying the
     * length of the 1D convolution window.
     */
    val kernelSize = try {
      getAsIntArray(params.getList(ModelNames.KERNEL_SIZE))

    } catch {
      case _: Throwable => Array.empty[Int]
    }
    /*
     * An integer or tuple/list of a single integer, specifying the
     * stride length of the convolution.
     *
     * Specifying any stride value != 1 is incompatible with specifying
     * any `dilation_rate` value != 1.
     */
    val strides = try {
      getAsIntArray(params.getList(ModelNames.STRIDES))

    } catch {
      case _: Throwable => Array(1, 1)
    }

    /*
     * Either 'valid', 'causal' or 'same'. Default is 'valid'.
     *
     * `valid` means "no padding".
     *
     * `same` results in padding the input such that the output
     * has the same length as the original input
     *
     * `causal` results in causal (dilated) convolutions, e.g.
     * output[t] does not depend on input[t+1:]. Useful when
     * modeling temporal data where the model should not violate
     * the temporal order.
     */
    val padding = getAsString(params, ModelNames.PADDING, "valid")
    /*
     * One of `channels_last` (default) or `channels_first`.
     *
     * The ordering of the dimensions in the inputs.
     *
     * `channels_last` corresponds to inputs with shape
     *
     * `(batch, height, width, channels)` while
     *
     * `channels_first` corresponds to inputs with shape
     * `(batch, channels, height, width)`.
     */
    val dataFormat = getAsString(params, ModelNames.DATA_FORMAT, "channels_first")

    /*
     * Activation function to use. Default is null. You can also
     * pass in corresponding string representations such as 'relu'
     * or 'sigmoid', etc. for simple activations in the factory method.
     */
    val activation: String = try {
      params.getString(ModelNames.ACTIVATION)

    } catch {
      case _: Throwable => null
    }
    /*
     * Whether to include a bias (i.e. make the layer affine rather
     * than linear). Default is true.
     */
    val bias = getAsBoolean(params, ModelNames.BIAS, default = true)
    /*
     * Initializer for the `kernel` weights matrix.
     */
    val kernelInitializer = getAsString(params, ModelNames.KERNEL_INITIALIZER, "glorot_uniform")
    /*
     * Initializer for the bias vector.
     */
    val biasInitializer = getAsString(params, ModelNames.BIAS_INITIALIZER, "zero")
    /*
     * Weight regularizer
     *
     * An instance of [[Regularizer]], (eg. L1 or L2 regularization),
     * applied to the input weights matrices. Default is null.
     *
     * NOTE: This is the kernel regularizer in Keras2
     */
    val kernelRegularizer = params2RegularizerW(params)
    /*
     * Bias regularizer
     *
     * An instance of [[Regularizer]], (eg. L1 or L2 regularization),
     * applied to the bias. Default is null.
     */
    val biasRegularizer = params2RegularizerB(params)

    keras2.layers.Conv2D(
      filters = filters,
      kernelSize = kernelSize,
      strides = strides,
      padding = padding,
      dataFormat = dataFormat,
      activation = activation,
      useBias = bias,
      kernelInitializer = kernelInitializer,
      biasInitializer = biasInitializer,
      kernelRegularizer = kernelRegularizer,
      biasRegularizer = biasRegularizer)

  }

  /**
   * Convolutional LSTM.
   *
   * Note that currently only 'same' padding is supported.
   *
   * The convolution kernel for this layer is a square kernel with
   * equal strides.
   *
   * The input of this layer should be 5D, i.e. (samples, time, channels,
   * rows, cols), and 'CHANNEL_FIRST' (dimOrdering='th') is expected.
   *
   * outputDimension: Number of convolution filters to use.
   * nbKernel:        Number of rows/columns in the convolution kernel. Square kernel.
   *
   */
  def config2ConvLSTM2D(layer: Config): ConvLSTM2D[Float] = {

    val params = layer.getConfig(ModelNames.PARAMS)

    /* Activation function to use. You can also pass in corresponding
     * string representations such as 'relu' or 'sigmoid', etc. for
     * simple activations in the factory method. Default is 'tanh'.
     */
    val activation: String = getAsString(params, ModelNames.ACTIVATION, "tanh")
    /*
     * Activation function for inner cells. You can also pass in corresponding
     * string representations such as 'relu' or 'sigmoid', etc. for simple
     * activations in the factory method. Default is 'hard_sigmoid'.
     */
    val innerActivation: String = getAsString(params, ModelNames.INNER_ACTIVATION, "hard_sigmoid")

    /* Either 'valid' or 'same'. Default is 'valid'. */
    val borderMode = getAsString(params, ModelNames.BORDER_MODE, "valid")

    /* Format of input data. Please use "CHANNEL_FIRST" (dimOrdering='th'). */
    val dimOrdering = getAsString(params, ModelNames.DIM_ORDERING, "th")

    /*
     * Whether the input sequence will be processed backwards.
     * Default is false.
     */
    val goBackwards = getAsBoolean(params, ModelNames.GO_BACKWARDS, default = false)

    /* Number of convolution filters to use. */
    val nbFilter: Int = params.getInt(ModelNames.NB_FILTER)

    /* Number of rows/columns in the convolution kernel. Square kernel. */
    val nbKernel: Int = params.getInt(ModelNames.NB_KERNEL)

    /*
     * Whether to return the full sequence or only return
     * the last output in the output sequence. Default is false.
     */
    val returnSequences = getAsBoolean(params, ModelNames.RETURN_SEQUENCES, default = false)

    /*
     * Factor by which to subsample output. Also called strides
     * elsewhere. Default is 1.
     */
    val subsample = getAsInt(params, ModelNames.SUBSAMPLE, 1)
    /*
     * Bias regularizer
     *
     *  An instance of [[Regularizer]], applied to the bias.
     *  Default is null.
     */
    val bRegularizer = params2RegularizerB(params)
    /*
     * An instance of [[Regularizer]], (eg. L1 or L2 regularization),
     * applied to the recurrent weights matrices. Default is null.
     */
    val uRegularizer = params2RegularizerU(params)

    /*
     * Weight regularizer
     *
     * An instance of [[Regularizer]], (eg. L1 or L2 regularization),
     * applied to the input weights matrices. Default is null.
     */
    val wRegularizer = params2RegularizerW(params)

    keras.layers.ConvLSTM2D(
      activation = activation,
      borderMode = borderMode,
      dimOrdering = dimOrdering,
      goBackwards = goBackwards,
      innerActivation = innerActivation,
      nbFilter = nbFilter,
      nbKernel = nbKernel,
      returnSequences = returnSequences,
      subsample = subsample,
      wRegularizer = wRegularizer,
      bRegularizer = bRegularizer,
      uRegularizer = uRegularizer)

  }

  def config2Dense_V1(layer: Config): Dense[Float] = {

    val params = layer.getConfig(ModelNames.PARAMS)

    /* The size of output dimension. */
    val outputDim: Int = params.getInt(ModelNames.OUTPUT_DIM)

    /* Activation function to use. */
    val activation: String = getAsString(params, ModelNames.ACTIVATION, null)

    /*
     * Weight regularizer
     *
     * An instance of [[Regularizer]], (eg. L1 or L2 regularization),
     * applied to the input weights matrices. Default is null.
     */
    val wRegularizer = params2RegularizerW(params)
    /*
     * Bias regularizer
     *
     * An instance of [[Regularizer]], (eg. L1 or L2 regularization),
     * applied to the bias. Default is null.
     */
    val bRegularizer = params2RegularizerB(params)
    /*
     * Whether to include a bias (i.e. make the layer affine rather
     * than linear). Default is true.
     */
    val bias = getAsBoolean(params, ModelNames.BIAS, default = true)

    keras.layers.Dense(
      outputDim = outputDim,
      activation = activation,
      wRegularizer = wRegularizer,
      bRegularizer = bRegularizer,
      bias = bias)

  }

  /*
   * A densely-connected NN layer. The most common input is 2D.
   */
  def config2Dense_V2(layer: Config): layers.Dense[Float] = {

    val params = layer.getConfig(ModelNames.PARAMS)

    /* The size of output dimension. */
    val units: Int = params.getInt(ModelNames.UNITS)

    /* Activation function to use. */
    val activation: String = getAsString(params, ModelNames.ACTIVATION, null)
    /*
     * Initializer for the `kernel` weights matrix. Default is Xavier.
     * You can also pass in corresponding string representations such
     * as 'glorot_uniform' or 'normal', etc. for simple init methods
     * in the factory method:
     *
     * - glorot_uniform
     * - one
     * - zero
     * - uniform
     * - normal
     */
    val kernelInitializer = getAsString(params, ModelNames.KERNEL_INITIALIZER, "glorot_uniform")
    /*
     * Weight regularizer
     *
     * An instance of [[Regularizer]], (eg. L1 or L2 regularization),
     * applied to the input weights matrices. Default is null.
     */
    val kernelRegularizer = params2RegularizerW(params)
    /*
     * Bias regularizer
     *
     * An instance of [[Regularizer]], (eg. L1 or L2 regularization),
     * applied to the bias. Default is null.
     */
    val biasRegularizer = params2RegularizerB(params)

    /*
     * Whether to include a bias (i.e. make the layer affine rather
     * than linear). Default is true.
     */
    val bias = getAsBoolean(params, ModelNames.BIAS, default = true)

    keras2.layers.Dense(
      units = units,
      activation = activation,
      kernelInitializer = kernelInitializer,
      kernelRegularizer = kernelRegularizer,
      biasRegularizer = biasRegularizer,
      useBias = bias
    )

  }

  def config2Dropout_V1(layer: Config): Dropout[Float] = {

    val params = layer.getConfig(ModelNames.PARAMS)

    /* Fraction of the input units to drop. Double between 0 and 1. */
    val p: Double = params.getDouble(ModelNames.P)

    keras.layers.Dropout(p = p)

  }

  /**
   * Applies Dropout to the input by randomly setting a fraction 'rate'
   * of input units to 0 at each update during training time in order
   * to prevent overfitting.
   */
  def config2Dropout_V2(layer: Config): layers.Dropout[Float] = {

    val params = layer.getConfig(ModelNames.PARAMS)

    /* Fraction of the input units to drop. Double between 0 and 1. */
    val rate: Double = params.getDouble(ModelNames.RATE)

    keras2.layers.Dropout(rate = rate)

  }

  def config2ELU(layer: Config): ELU[Float] = {

    val params = layer.getConfig(ModelNames.PARAMS)

    /* Scale for the negative factor. Default is 1.0. */

    val alpha = getAsDouble(params, ModelNames.ALPHA, 1.0)
    keras.layers.ELU(alpha = alpha)

  }

  /*
   * Flatten the results to feed into a dense layer
   */
  def config2Flatten_V1(): Flatten[Float] = {
    keras.layers.Flatten()
  }

  def config2Flatten_V2(layer: Config): layers.Flatten[Float] = {
    keras2.layers.Flatten()
  }

  def config2GlobalAvgPool1D_V1(layer: Config): GlobalAveragePooling1D[Float] = {
    keras.layers.GlobalAveragePooling1D()
  }

  def config2GlobalAvgPool1D_V2(layer: Config): layers.GlobalAveragePooling1D[Float] = {
    keras2.layers.GlobalAveragePooling1D()
  }

  def config2GlobalMaxPool1D_V1(layer: Config): GlobalMaxPooling1D[Float] = {
    keras.layers.GlobalMaxPooling1D()
  }

  def config2GlobalMaxPool1D_V2(layer: Config): layers.GlobalMaxPooling1D[Float] = {
    keras2.layers.GlobalMaxPooling1D()
  }

  /* Gated Recurrent Unit architecture. */
  def config2GRU(layer: Config): GRU[Float] = {

    val params = layer.getConfig(ModelNames.PARAMS)

    /*
     * Hidden unit size. Dimension of internal projections and
     * final output.
     */
    val outputDim = params.getInt(ModelNames.OUTPUT_DIM)
    /*
     * Activation function to use. Default is 'tanh'.
     */
    val activation = getAsString(params, ModelNames.ACTIVATION, "tanh")
    /*
     * Activation function for inner cells. Default is 'hard_sigmoid'.
     */
    val innerActivation = getAsString(params, ModelNames.INNER_ACTIVATION, "hard_sigmoid")
    /*
     * Whether to return the full sequence or only return
     * the last output in the output sequence. Default is false.
     */
    val returnSequences = getAsBoolean(params, ModelNames.RETURN_SEQUENCES, default = false)
    /*
     * Whether the input sequence will be processed backwards. Default is false.
     */
    val goBackwards = getAsBoolean(params, ModelNames.GO_BACKWARDS, default = false)

    /*
     * Weight regularizer
     *
     * An instance of [[Regularizer]], (eg. L1 or L2 regularization),
     * applied to the input weights matrices. Default is null.
     */
    val wRegularizer = params2RegularizerW(params)
    /*
     * An instance of [[Regularizer]], applied the recurrent weights
     * matrices. Default is null.
     */
    val uRegularizer = params2RegularizerU(params)
    /*
     * Bias regularizer
     *
     * An instance of [[Regularizer]], (eg. L1 or L2 regularization),
     * applied to the bias. Default is null.
     */
    val bRegularizer = params2RegularizerB(params)

    keras.layers.GRU(
      outputDim = outputDim,
      activation = activation,
      innerActivation = innerActivation,
      returnSequences = returnSequences,
      goBackwards = goBackwards,
      wRegularizer = wRegularizer,
      uRegularizer = uRegularizer,
      bRegularizer = bRegularizer)

  }

  def config2LSTM(layer: Config):LSTM[Float] = {

    val params = layer.getConfig(ModelNames.PARAMS)

    /*
     * Hidden unit size. Dimension of internal projections
     * and final output.
     */
    val outputDim = params.getInt(ModelNames.OUTPUT_DIM)

    /* Activation function to use. */
    val activation = getAsString(params, ModelNames.ACTIVATION, "tanh")

    /* Activation function for inner cells. */
    val innerActivation = getAsString(params, ModelNames.INNER_ACTIVATION, "hard_sigmoid")

    /*
     * Whether to return the full sequence or only return
     * the last output in the output sequence. Default is false.
     */
    val returnSequences = getAsBoolean(params, ModelNames.RETURN_SEQUENCES, default = false)

    /*
     * Whether the input sequence will be processed backwards.
     * Default is false.
     */
    val goBackwards = getAsBoolean(params, ModelNames.GO_BACKWARDS, default = false)

    keras.layers.LSTM(
      outputDim = outputDim,
      activation = activation,
      innerActivation = innerActivation,
      returnSequences = returnSequences,
      goBackwards = goBackwards)

  }

  def config2MaxPooling1D_V1(layer: Config): MaxPooling1D[Float] = {

    val params = layer.getConfig(ModelNames.PARAMS)

    /*
     * Size of the region to which max pooling is applied.
     * Integer. Default is 2.
     */
    val poolLength = getAsInt(params, ModelNames.POOL_LENGTH, 2)
    /*
     * Factor by which to downscale. Integer, or -1. 2 will
     * halve the input. If -1, it will default to poolLength.
     * Default is -1.
     */
    val stride = getAsInt(params, ModelNames.STRIDE, -1)

    /* Either 'valid' or 'same'. Default is 'valid'. */
    val borderMode = getAsString(params, ModelNames.BORDER_MODE, "valid")

    keras.layers.MaxPooling1D(
      poolLength = poolLength,
      stride = stride,
      borderMode = borderMode)

  }

  def config2MaxPooling1D_V2(layer: Config): layers.MaxPooling1D[Float] = {

    val params = layer.getConfig(ModelNames.PARAMS)

    /*
     * Size of the region to which max pooling is applied.
     * Integer. Default is 2.
     */
    val poolSize = getAsInt(params, ModelNames.POOL_SIZE, 2)
    /*
     * Factor by which to downscale. Integer, or -1. 2 will
     * halve the input. If -1, it will default to poolLength.
     * Default is -1.
     */
    val poolStrides = getAsInt(params, ModelNames.POOL_STRIDES, -1)

    /* Either 'valid' or 'same'. Default is 'valid'. */
    val padding = getAsString(params, ModelNames.PADDING, "valid")

    keras2.layers.MaxPooling1D(
      poolSize = poolSize,
      strides = poolStrides,
      padding = padding)

  }

  def config2MaxPooling2D(layer: Config): MaxPooling2D[Float] = {

    val params = layer.getConfig(ModelNames.PARAMS)
    /*
     * Int tuple of length 2 corresponding to the downscale
     * vertically and horizontally. Default is (2, 2), which
     * will halve the image in each dimension.
     */
    val poolSize = getAsIntArray(params.getList(ModelNames.POOL_SIZE))
    keras.layers.MaxPooling2D(poolSize = (poolSize(0), poolSize(1)))

  }

  /*
   * Repeats the input n times. The input of this layer should be 2D.
   */

  def config2RepeatVector(layer: Config): RepeatVector[Float] = {

    val params = layer.getConfig(ModelNames.PARAMS)
    /*
     * Repetition factor. Integer.
     */
    val n = params.getInt(ModelNames.N)
    keras.layers.RepeatVector(n = n)

  }

  def config2Shape(input: ConfigList): Shape = {

    try {

      val dimensions = getAsIntArray(input)
      val shape = Shape(dimensions: _*)
      shape

    } catch {
      case _: Throwable => throw new Exception("Input shape cannot be built.")
    }

  }

  def params2RegularizerB(params: Config): L1L2Regularizer[Float] = {
    /*
     * Bias regularizer
     *
     * An instance of [[Regularizer]], (eg. L1 or L2 regularization),
     * applied to the bias. Default is null.
     */
    val bRegularizer = try {

      val bReg = params.getConfig(ModelNames.B_REGULARIZER)
      config2Regularizer(bReg)

    } catch {
      case _: Throwable => null
    }

    bRegularizer

  }

  def params2RegularizerU(params: Config): L1L2Regularizer[Float] = {
    /*
     * An instance of [[Regularizer]], (eg. L1 or L2 regularization),
 		 * applied to the recurrent weights matrices. Default is null.
     */
    val uRegularizer = try {

      val uReg = params.getConfig(ModelNames.U_REGULARIZER)
      config2Regularizer(uReg)

    } catch {
      case _: Throwable => null
    }

    uRegularizer

  }

  def params2RegularizerW(params: Config): L1L2Regularizer[Float] = {
    /*
     * Weight regularizer
     *
     * An instance of [[Regularizer]], (eg. L1 or L2 regularization),
     * applied to the input weights matrices. Default is null.
     *
     * NOTE: This is the kernel regularizer in Keras2
     */
    val wRegularizer = try {

      val wReg = params.getConfig(ModelNames.W_REGULARIZER)
      config2Regularizer(wReg)

    } catch {
      case _: Throwable => null
    }

    wRegularizer

  }

}
