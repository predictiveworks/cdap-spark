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

import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
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
@Name("DTClassifer")
@Description("A building stage for an Apache Spark based Decision Tree classifier model. This stages expects a dataset that contains at least 2 fields to train the classifier model: "
		+ "A field that contains the features formatted as an array of double values, and, another field that class or label value also specified as a double value.")
public class DTClassifier extends BaseClassifierSink {

	private static final long serialVersionUID = -4324297354460233205L;

	public DTClassifier(DTClassifierConfig config) {
		this.config = config;
		this.className = DTClassifier.class.getName();
	}

	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
		super.configurePipeline(pipelineConfigurer);

		/* Validate configuration */
		((DTClassifierConfig)config).validate();

		/* Validate schema */
		StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
		inputSchema = stageConfigurer.getInputSchema();
		if (inputSchema != null)
			validateSchema(inputSchema, config);

	}

	@Override
	public void compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {
		
		DTClassifierConfig classifierConfig = (DTClassifierConfig)config;
		/*
		 * STEP #1: Extract parameters and train classifier model
		 */
		String featuresCol = classifierConfig.featuresCol;
		String labelCol = classifierConfig.labelCol;

		Map<String, Object> params = classifierConfig.getParamsAsMap();
		/*
		 * The vectorCol specifies the internal column that has to be built from the
		 * featuresCol and that is used for training purposes
		 */
		String vectorCol = "_vector";
		/*
		 * Prepare provided dataset by vectorizing the feature column which is specified
		 * as Array[Numeric]
		 */
		DTTrainer trainer = new DTTrainer();
		Dataset<Row> vectorset = trainer.vectorize(source, featuresCol, vectorCol);
		/*
		 * Split the vectorset into a train & test dataset for later classification
		 * evaluation
		 */
		Dataset<Row>[] splitted = vectorset.randomSplit(classifierConfig.getSplits());

		Dataset<Row> trainset = splitted[0];
		Dataset<Row> testset = splitted[1];

		DecisionTreeClassificationModel model = trainer.train(trainset, vectorCol, labelCol, params);
		/*
		 * STEP #2: Compute accuracy of the trained classification model
		 */
		String predictionCol = "_prediction";
		model.setPredictionCol(predictionCol);

		Dataset<Row> predictions = model.transform(testset);
		/*
		 * This Decision Tree plugin leverages the multiclass evaluator independent of
		 * whether the algorithm is used for binary classification or not.
		 */
		MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator();
		evaluator.setLabelCol(labelCol);
		evaluator.setPredictionCol(predictionCol);

		String metricName = "accuracy";
		evaluator.setMetricName(metricName);

		double accuracy = evaluator.evaluate(predictions);
		/*
		 * The accuracy coefficent is specified as JSON metrics for this classification
		 * model and stored by the DTClassifierManager
		 */
		Map<String, Object> metrics = new HashMap<>();

		metrics.put("name", metricName);
		metrics.put("coefficient", accuracy);
		/*
		 * STEP #3: Store trained classification model including its associated
		 * parameters and metrics
		 */
		String paramsJson = classifierConfig.getParamsAsJSON();
		String metricsJson = new Gson().toJson(metrics);

		String modelName = classifierConfig.modelName;
		new DTClassifierManager().save(modelFs, modelMeta, modelName, paramsJson, metricsJson, model);

	}

	public static class DTClassifierConfig extends BaseClassifierConfig {

		private static final long serialVersionUID = -5216062714694933745L;

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

		public DTClassifierConfig() {

			dataSplit = "70:30";

			impurity = "gini";
			minInfoGain = 0D;

			maxBins = 32;
			maxDepth = 5;

		}

		@Override
		public Map<String, Object> getParamsAsMap() {

			Map<String, Object> params = new HashMap<>();

			params.put("impurity", impurity);
			params.put("minInfoGain", minInfoGain);

			params.put("maxBins", maxBins);
			params.put("maxDepth", maxDepth);

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

		}

	}
}
