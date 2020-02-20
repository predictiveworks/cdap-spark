package de.kp.works.ml.classification;
/*
 * Copyright (c) 2019 Dr. Krusche & Partner PartG. All rights reserved.
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

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.google.common.base.Strings;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import co.cask.cdap.etl.api.batch.SparkSink;

import de.kp.works.core.classifier.ClassifierConfig;
import de.kp.works.core.classifier.ClassifierSink;

@Plugin(type = SparkSink.PLUGIN_TYPE)
@Name("MLPClassifer")
@Description("A building stage for an Apache Spark ML Multi-Layer Perceptron classifier model. This stage expects "
		+ "a dataset with at least two fields to train the model: One as an array of numeric values, and, "  
		+ "another that describes the class or label value as numeric value.")
public class MLPClassifier extends ClassifierSink {

	private static final long serialVersionUID = -7445401286046769822L;
	
	private MLPClassifierConfig config;

	public MLPClassifier(MLPClassifierConfig config) {
		this.config = config;
	}

	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
		super.configurePipeline(pipelineConfigurer);

		/* Validate configuration */
		config.validate();
		
		/* Validate schema */
		StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
		inputSchema = stageConfigurer.getInputSchema();
		if (inputSchema != null)
			validateSchema(inputSchema);

	}
	
	@Override
	public void compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {
		
		/*
		 * STEP #1: Extract parameters and train classifier model
		 */
		String featuresCol = config.featuresCol;
		String labelCol = config.labelCol;

		Map<String, Object> params = config.getParamsAsMap();
		String paramsJson = config.getParamsAsJSON();
		/*
		 * The vectorCol specifies the internal column that has
		 * to be built from the featuresCol and that is used for
		 * training purposes
		 */
		String vectorCol = "_vector";
		/*
		 * Prepare provided dataset by vectorizing the feature
		 * column which is specified as Array[Numeric]
		 */
		MLPTrainer trainer = new MLPTrainer();
		Dataset<Row> vectorset = trainer.vectorize(source, featuresCol, vectorCol);
		/*
		 * Split the vectorset into a train & test dataset for
		 * later classification evaluation
		 */
	    Dataset<Row>[] splitted = vectorset.randomSplit(config.getSplits());
		
	    Dataset<Row> trainset = splitted[0];
	    Dataset<Row> testset = splitted[1];

	    MultilayerPerceptronClassificationModel model = trainer.train(trainset, vectorCol, labelCol, params);
		/*
		 * STEP #2: Evaluate classification model and compute
		 * approved list of metrics
		 */
	    String predictionCol = "_prediction";
	    model.setPredictionCol(predictionCol);

	    Dataset<Row> predictions = model.transform(testset);
	    String metricsJson = Evaluator.evaluate(predictions, labelCol, predictionCol);
		/*
		 * STEP #3: Store trained classification model including
		 * its associated parameters and metrics
		 */		
		String modelName = config.modelName;
		new MLPRecorder().track(context, modelName, paramsJson, metricsJson, model);

	}

	@Override
	public void validateSchema(Schema inputSchema) {
		config.validateSchema(inputSchema);
	}

	public static class MLPClassifierConfig extends ClassifierConfig {

		private static final long serialVersionUID = 7824534239299625292L;

		@Description("The comma-separated list of the sizes of the layers from the input to the output layer. "
				+ "For example: 780,100,10 means 780 inputs, one hidden layer with 100 neurons and an output "
				+ "layer with 10 neurons. At least 2 layers (input, output) must be specified.")
		@Macro
		public String layers;
		
		@Description("The nonnegative block size for stacking input data in matrices to speed up the computation. "
				+ "Data is stacked within partitions. If block size is more than remaining data in a partition then "
				+ "it is adjusted to the size of this data. Recommended size is between 10 and 1000. Default is 128.")
		@Macro
		public Integer blockSize;

		@Description("The maximum number of iterations to train the Multi-Layer Perceptron model. Default is 100.")
		@Macro
		public Integer maxIter;

		@Description("The learning rate for shrinking the contribution of each estimator. Must be in interval (0, 1]. Default is 0.03.")
		@Macro
		public Double stepSize;

		@Description("The solver algorithm for optimization. Supported options are 'gd' (minibatch gradient descent) and 'l-bfgs'. Default is 'l-bfgs'.")
		@Macro
		public String solver;
		
		@Description("The positive convergence tolerance of iterations. Smaller values wuth lead to higher accuracy with the cost "
				+ "of more iterations. Default is 1e-6.")
		@Macro
		public Double tol;

		public MLPClassifierConfig() {

			dataSplit = "70:30";
			blockSize = 128;

			maxIter = 100;
			stepSize = 0.03;
			
			solver = "l-bfgs";
			tol = 1e-6;

		}
		
		@Override
		public Map<String, Object> getParamsAsMap() {
			
			Map<String, Object> params = new HashMap<>();
			
			params.put("layers", layers);
			params.put("blockSize", blockSize);

			params.put("maxIter", maxIter);
			params.put("stepSize", stepSize);

			params.put("solver", solver);
			params.put("tol", tol);

			return params;
		
		}

		public void validate() {
			super.validate();
			
			/** PARAMETERS **/
			if (blockSize <= 0)
				throw new IllegalArgumentException(String.format(
						"[%s] The block size must be nonnegative.", this.getClass().getName()));
			if (maxIter < 1)
				throw new IllegalArgumentException(String.format(
						"[%s] The maximum number of iterations must be at least 1.", this.getClass().getName()));

			if (stepSize <= 0D || stepSize > 1D)
				throw new IllegalArgumentException(
						String.format("[%s] The learning rate must be in interval (0, 1].", this.getClass().getName()));

			if (tol <= 0D)
				throw new IllegalArgumentException(
						String.format("[%s] The iteration tolerance must be positive.", this.getClass().getName()));
			
			if (Strings.isNullOrEmpty(layers))
				throw new IllegalArgumentException(
						String.format("[%s] The layers of the neural net must be specified.",
								this.getClass().getName()));
			
			String[] layerArray = layers.split(",");
			if (layerArray.length < 2)
				throw new IllegalArgumentException(
						String.format("[%s] At least 2 layers must be specified.", this.getClass().getName()));
						
		}
		
	}
}
