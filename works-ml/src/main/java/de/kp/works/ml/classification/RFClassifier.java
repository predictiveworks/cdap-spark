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

import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

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
@Name("RFClassifer")
@Description("A building stage for an Apache Spark based Random Forest Trees classifier model.")
public class RFClassifier extends BaseClassifierSink {

	private static final long serialVersionUID = -6423500795219581617L;

	public RFClassifier(RFClassifierConfig config) {
		this.config = config;
		this.className = RFClassifier.class.getName();
	}

	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
		super.configurePipeline(pipelineConfigurer);

		/* Validate configuration */
		((RFClassifierConfig)config).validate();

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
		 * The vectorCol specifies the internal column that has to be built from the
		 * featuresCol and that is used for training purposes
		 */
		String vectorCol = "_vector";
		/*
		 * Prepare provided dataset by vectorizing the feature column which is specified
		 * as Array[Numeric]
		 */
		RFTrainer trainer = new RFTrainer();
		Dataset<Row> vectorset = trainer.vectorize(source, featuresCol, vectorCol);
		/*
		 * Split the vectorset into a train & test dataset for later classification
		 * evaluation
		 */
		Dataset<Row>[] splitted = vectorset.randomSplit(config.getSplits());

		Dataset<Row> trainset = splitted[0];
		Dataset<Row> testset = splitted[1];

		RandomForestClassificationModel model = trainer.train(trainset, vectorCol, labelCol, params);
		/*
		 * STEP #2: Compute accuracy of the trained classification model
		 */
		String predictionCol = "_prediction";
		model.setPredictionCol(predictionCol);

		Dataset<Row> predictions = model.transform(testset);
		/*
		 * This Random Forest Classifier plugin leverages the multiclass evaluator
		 * independent of whether the algorithm is used for binary classification or
		 * not.
		 */
		MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator();
		evaluator.setLabelCol(labelCol);
		evaluator.setPredictionCol(predictionCol);

		String metricName = "accuracy";
		evaluator.setMetricName(metricName);

		double accuracy = evaluator.evaluate(predictions);
		/*
		 * The accuracy coefficent is specified as JSON metrics for this classification
		 * model and stored by the RFClassifierManager
		 */
		Map<String, Object> metrics = new HashMap<>();

		metrics.put("name", metricName);
		metrics.put("coefficient", accuracy);
		/*
		 * STEP #3: Store trained classification model including its associated
		 * parameters and metrics
		 */
		String paramsJson = config.getParamsAsJSON();
		String metricsJson = new Gson().toJson(metrics);

		String modelName = config.modelName;
		new RFClassifierManager().save(modelFs, modelMeta, modelName, paramsJson, metricsJson, model);

	}
	/*
	 * The configuration for Random Forest classification & regression
	 * models are identical
	 */
	public static class RFClassifierConfig extends BaseClassifierConfig {

		private static final long serialVersionUID = -6477522356251530089L;

		@Description("Impurity is a criterion how to calculate information gain. Supported values: 'entropy' and 'gini'. Default is 'gini'.")
		@Macro
		public String impurity;

		@Description("The maximum number of bins used for discretizing continuous features and for choosing how to split "
				+ " on features at each node. More bins give higher granularity. Must be at least 2. Default is 32.")
		@Macro
		public Integer maxBins;

		@Description("Nonnegative value that maximum depth of the tree. E.g. depth 0 means 1 leaf node; "
				+ " depth 1 means 1 internal node + 2 leaf nodes. Default is 5.")
		@Macro
		public Integer maxDepth;

		@Description("The minimum information gain for a split to be considered at a tree node. The value should be at least 0.0. Default is 0.0.")
		@Macro
		public Double minInfoGain;

		@Description("The number of trees to train the model. Default is 20.")
		@Macro
		public Integer numTrees;

		public RFClassifierConfig() {

			dataSplit = "70:30";
			impurity = "gini";

			maxBins = 32;
			maxDepth = 5;

			minInfoGain = 0D;
			numTrees = 20;

		}

		@Override
		public Map<String, Object> getParamsAsMap() {

			Map<String, Object> params = new HashMap<>();
			params.put("impurity", impurity);

			params.put("maxBins", maxBins);
			params.put("maxDepth", maxDepth);

			params.put("minInfoGain", minInfoGain);
			params.put("numTrees", numTrees);

			return params;

		}

		public void validate() {
			super.validate();

			/** PARAMETERS **/
			if (maxBins < 2)
				throw new IllegalArgumentException(
						String.format("[%s] The maximum bins must be at least 2.", this.getClass().getName()));

			if (maxDepth < 0)
				throw new IllegalArgumentException(
						String.format("[%s] The maximum depth must be nonnegative.", this.getClass().getName()));

			if (minInfoGain < 0D)
				throw new IllegalArgumentException(String
						.format("[%s] The minimum information gain must be at least 0.0.", this.getClass().getName()));

			if (numTrees < 1)
				throw new IllegalArgumentException(
						String.format("[%s] The number of trees must be at least 1.", this.getClass().getName()));

		}

	}
}
