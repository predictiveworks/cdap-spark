package de.kp.works.ml.regression;
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

import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.regression.GBTRegressionModel;
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
import de.kp.works.core.RegressorConfig;
import de.kp.works.core.RegressorSink;

@Plugin(type = "sparksink")
@Name("GBTRegressor")
@Description("A building stage for an Apache Spark based Gradient-Boosted Trees regressor model.")
public class GBTRegressor extends RegressorSink {

	private static final long serialVersionUID = -8027323270352311072L;
	
	public GBTRegressor(GBTRegressorConfig config) {
		this.config = config;
		this.className = GBTRegressor.class.getName();
	}

	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
		super.configurePipeline(pipelineConfigurer);

		/* Validate configuration */
		((GBTRegressorConfig)config).validate();
		
		/* Validate schema */
		StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
		inputSchema = stageConfigurer.getInputSchema();
		if (inputSchema != null)
			validateSchema(inputSchema, config);

	}
	
	@Override
	public void compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {
		
		GBTRegressorConfig regressorConfig = (GBTRegressorConfig)config;
		/*
		 * STEP #1: Extract parameters and train regression model
		 */
		String featuresCol = regressorConfig.featuresCol;
		String labelCol = regressorConfig.labelCol;

		Map<String, Object> params = regressorConfig.getParamsAsMap();
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
		GBTTrainer trainer = new GBTTrainer();
		Dataset<Row> vectorset = trainer.vectorize(source, featuresCol, vectorCol);
		/*
		 * Split the vectorset into a train & test dataset for
		 * later regression evaluation
		 */
	    Dataset<Row>[] splitted = vectorset.randomSplit(regressorConfig.getSplits());
		
	    Dataset<Row> trainset = splitted[0];
	    Dataset<Row> testset = splitted[1];

	    GBTRegressionModel model = trainer.train(trainset, vectorCol, labelCol, params);
		/*
		 * STEP #2: Compute accuracy of the trained regression
		 * model
		 */
	    String predictionCol = "_prediction";
	    model.setPredictionCol(predictionCol);

	    Dataset<Row> predictions = model.transform(testset);

	    RegressionEvaluator evaluator = new RegressionEvaluator();
	    evaluator.setLabelCol(labelCol);
	    evaluator.setPredictionCol(predictionCol);
	    
	    String metricName = "rmse";
	    evaluator.setMetricName(metricName);
	    
	    double accuracy = evaluator.evaluate(predictions);
		/*
		 * The accuracy coefficent is specified as JSON metrics for
		 * this regression model and stored by the GBTRegressorManager
		 */
		Map<String,Object> metrics = new HashMap<>();
		
		metrics.put("name", metricName);
		metrics.put("coefficient", accuracy);
		/*
		 * STEP #3: Store trained regression model including its associated
		 * parameters and metrics
		 */
		String paramsJson = regressorConfig.getParamsAsJSON();
		String metricsJson = new Gson().toJson(metrics);
		
		String modelName = regressorConfig.modelName;
		new GBTRegressorManager().save(modelFs, modelMeta, modelName, paramsJson, metricsJson, model);

	}

	public static class GBTRegressorConfig extends RegressorConfig {

		private static final long serialVersionUID = 8655177932723069092L;

		/*
		 * Impurity is set to 'variance' and cannot be changed; therefore no
		 * external parameter is provided
		 */

		@Description("The type of the loss function the Gradient-Boosted Trees algorithm tries to minimize. Supported values are 'squared' (L2) and 'absolute' (L1). Default is 'squared'.")
		@Macro
		public String lossType;
		
		@Description("The maximum number of bins used for discretizing continuous features and for choosing how to split "
				+ " on features at each node. More bins give higher granularity. Must be at least 2. Default is 32.")
		@Macro
		public Integer maxBins;
		
		@Description("Nonnegative value that maximum depth of the tree. E.g. depth 0 means 1 leaf node; "
				+" depth 1 means 1 internal node + 2 leaf nodes. Default is 5.")
		@Macro
		public Integer maxDepth;

		@Description("The maximum number of iterations to train the Gradient-Boosted Trees model. Default is 20.")
		@Macro
		public Integer maxIter;

		@Description("The minimum information gain for a split to be considered at a tree node. The value should be at least 0.0. Default is 0.0.")
		@Macro
		public Double minInfoGain;
		
		@Description("The learning rate for shrinking the contribution of each estimator. Must be in interval (0, 1]. Default is 0.1")
		@Macro
		public Double stepSize;
		
		public GBTRegressorConfig() {

			dataSplit = "70:30";
			
			lossType = "squared";
			minInfoGain = 0D;

			maxBins = 32;
			maxDepth = 5;
			
			maxIter = 20;
			stepSize = 0.1;

		}
	    
		@Override
		public Map<String, Object> getParamsAsMap() {
			
			Map<String, Object> params = new HashMap<>();

			params.put("lossType", lossType);
			params.put("minInfoGain", minInfoGain);

			params.put("maxBins", maxBins);
			params.put("maxDepth", maxDepth);

			params.put("maxIter", maxIter);
			params.put("stepSize", stepSize);

			params.put("dataSplit", dataSplit);
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

			if (maxIter < 1)
				throw new IllegalArgumentException(String.format(
						"[%s] The maximum number of iterations must be at least 1.", this.getClass().getName()));

			if (stepSize <= 0D || stepSize > 1D)
				throw new IllegalArgumentException(
						String.format("[%s] The learning rate must be in interval (0, 1].", this.getClass().getName()));
			
		}
		
	}
}
