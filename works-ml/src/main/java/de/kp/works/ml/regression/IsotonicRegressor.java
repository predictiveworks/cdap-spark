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
import org.apache.spark.ml.regression.IsotonicRegressionModel;
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
import de.kp.works.core.BaseRegressorConfig;
import de.kp.works.core.BaseRegressorSink;

@Plugin(type = "sparksink")
@Name("IsotonicRegressor")
@Description("A building stage for an Apache Spark based Isotonic regressor model.")
public class IsotonicRegressor extends BaseRegressorSink {

	private static final long serialVersionUID = 185956615279200366L;
	
	public IsotonicRegressor(IsotonicConfig config) {
		this.config = config;
		this.className = IsotonicRegressor.class.getName();
	}

	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
		super.configurePipeline(pipelineConfigurer);

		/* Validate configuration */
		((IsotonicConfig)config).validate();
		
		/* Validate schema */
		StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
		inputSchema = stageConfigurer.getInputSchema();
		if (inputSchema != null)
			validateSchema(inputSchema, config);

	}
	
	@Override
	public void compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {
		
		IsotonicConfig regressorConfig = (IsotonicConfig)config;
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
		IsotonicTrainer trainer = new IsotonicTrainer();
		Dataset<Row> vectorset = trainer.vectorize(source, featuresCol, vectorCol);
		/*
		 * Split the vectorset into a train & test dataset for
		 * later regression evaluation
		 */
	    Dataset<Row>[] splitted = vectorset.randomSplit(regressorConfig.getSplits());
		
	    Dataset<Row> trainset = splitted[0];
	    Dataset<Row> testset = splitted[1];

	    IsotonicRegressionModel model = trainer.train(trainset, vectorCol, labelCol, params);
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
		 * this regression model and stored by the IsotonicRegressorManager
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
		new IsotonicRegressorManager().save(modelFs, modelMeta, modelName, paramsJson, metricsJson, model);

	}

	public static class IsotonicConfig extends BaseRegressorConfig {
		  
		private static final long serialVersionUID = -4928234679795163044L;
		
		@Description("This indicator determines whether whether the output sequence should be 'isotonic' (increasing) or 'antitonic' (decreasing). Default is 'isotonic'.")
		@Macro
		public String isotonic;

		@Description("The nonnegative index of the feature. Default is 0.")
		@Macro
		public Integer featureIndex;
		
		public IsotonicConfig() {

			dataSplit = "70:30";
			
			isotonic = "isotonic";
			featureIndex = 0;
			
		}
	    
		@Override
		public Map<String, Object> getParamsAsMap() {
			
			Map<String, Object> params = new HashMap<>();

			params.put("isotonic", isotonic);
			params.put("featureIndex", featureIndex);

			return params;
		
		}

		public void validate() {
			super.validate();

			/** PARAMETERS **/
			if (featureIndex < 0)
				throw new IllegalArgumentException(String.format(
						"[%s] The feature index must be nonnegative.", this.getClass().getName()));
			
		}
				
	}

}
