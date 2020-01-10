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
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.google.common.base.Strings;
import com.google.gson.Gson;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import de.kp.works.core.BaseClassifierConfig;
import de.kp.works.core.BaseClassifierSink;

@Plugin(type = "sparksink")
@Name("MLPClassifer")
@Description("A building stage for an Apache Spark based Multilayer Perceptron classifier model.")
public class MLPClassifier extends BaseClassifierSink {

	private static final long serialVersionUID = -7445401286046769822L;
	
	public MLPClassifier(MLPClassifierConfig config) {
		this.config = config;
		this.className = MLPClassifier.class.getName();
	}

	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
		super.configurePipeline(pipelineConfigurer);

		/* Validate configuration */
		((MLPClassifierConfig)config).validate();
		
		/* Validate schema */
		StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
		inputSchema = stageConfigurer.getInputSchema();
		if (inputSchema != null)
			validateSchema(inputSchema, config);

	}
	
	@Override
	public void compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {
		/*
		 * STEP #1: Extract parameters and train classifier model
		 */
		String featuresCol = config.featuresCol;
		String labelCol = config.labelCol;

		Map<String, Object> params = config.getParamsAsMap();
		/*
		 * The vectorCol specifies the internal column that has
		 * to be built from the featuresCol and that is used for
		 * training purposes
		 */
		String vectorCol = "_vector";
		/*
		 * Prepare provided dataset by vectorizing the feature
		 * column which is specified as Array[Double]
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
		 * STEP #2: Compute accuracy of the trained classification
		 * model
		 */
	    String predictionCol = "_prediction";
	    model.setPredictionCol(predictionCol);

	    Dataset<Row> predictions = model.transform(testset);
	    /*
	     * This Multilayer perceptron plugin leverages the multiclass evaluator
	     * independent of whether the algorithm is used for binary classification
	     * or not.
	     */
	    MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator();
	    evaluator.setLabelCol(labelCol);
	    evaluator.setPredictionCol(predictionCol);
	    
	    String metricName = "accuracy";
	    evaluator.setMetricName(metricName);
	    
	    double accuracy = evaluator.evaluate(predictions);
		/*
		 * The accuracy coefficent is specified as JSON
		 * metrics for this classification model and stored
		 * by the MLPClassifierManager
		 */
		Map<String,Object> metrics = new HashMap<>();
		
		metrics.put("name", metricName);
		metrics.put("coefficient", accuracy);
		/*
		 * STEP #3: Store trained classification model 
		 * including its associated parameters and metrics
		 */
		String paramsJson = config.getParamsAsJSON();
		String metricsJson = new Gson().toJson(metrics);
		
		String modelName = config.modelName;
		new MLPClassifierManager().save(modelFs, modelMeta, modelName, paramsJson, metricsJson, model);

	}

	public static class MLPClassifierConfig extends BaseClassifierConfig {

		private static final long serialVersionUID = 7824534239299625292L;

		@Description("The comma-separated list of the sizes of the layers from the input to the output layer. "
				+ "For example: 780,100,10 means 780 inputs, one hidden layer with 100 neurons and an output "
				+ "layer with 10 neuros. At least 2 layers (input, output) must be specified.")
		@Macro
		public String layers;
		
		@Description("The nonnegative block size for stacking input data in matrices to speed up the computation. "
				+ "Data is stacked within partitions. If block size is more than remaining data in a partition then "
				+ "it is adjusted to the size of this data. Recommended size is between 10 and 1000. Default is 128.")
		@Macro
		public Integer blockSize;

		@Description("The maximum number of iterations to train the Multilayer Perceptron model. Default is 100.")
		@Macro
		public Integer maxIter;

		@Description("The learning rate for shrinking the contribution of each estimator. Must be in interval (0, 1]. Default is 0.03.")
		@Macro
		public Double stepSize;

		@Description("The solver algorithm for optimization. Supported options are 'gd' (minibatch gradient descent) and 'l-bfgs'. Default is 'l-bfgs'.")
		@Macro
		public String solver;
		
		@Description("The positive convergence tolerance of iterations. Smaller values wuth lead to higher accuracy with the cost "
				+ "of more iterations. Default is 1e-6")
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
			
			if (!Strings.isNullOrEmpty(layers))
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
