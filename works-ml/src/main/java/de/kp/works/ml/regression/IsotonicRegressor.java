package de.kp.works.ml.regression;
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

import java.util.HashMap;
import java.util.Map;

import de.kp.works.core.recording.regression.IsotonicRecorder;
import org.apache.spark.ml.regression.IsotonicRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import io.cdap.cdap.etl.api.batch.SparkSink;

import de.kp.works.core.recording.RegressorEvaluator;
import de.kp.works.core.regressor.RegressorConfig;
import de.kp.works.core.regressor.RegressorSink;

@Plugin(type = SparkSink.PLUGIN_TYPE)
@Name("IsotonicRegressor")
@Description("A building stage for an Apache Spark ML Isotonic Regression (regressor) model. This stage expects "
		+ "a dataset with at least two fields to train the model: One as an array of numeric values, and, " 
		+ "another that describes the class or label value as numeric value.")
public class IsotonicRegressor extends RegressorSink {

	private static final long serialVersionUID = 185956615279200366L;
	
	private final IsotonicConfig config;
	
	public IsotonicRegressor(IsotonicConfig config) {
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
		 * STEP #1: Extract parameters and train regression model
		 */
		String featuresCol = config.featuresCol;
		String labelCol = config.labelCol;

		Map<String, Object> params = config.getParamsAsMap();
		String modelParams = config.getParamsAsJSON();
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
	    Dataset<Row>[] splitted = vectorset.randomSplit(config.getSplits());
		
	    Dataset<Row> trainset = splitted[0];
	    Dataset<Row> testset = splitted[1];

	    IsotonicRegressionModel model = trainer.train(trainset, vectorCol, labelCol, params);
		/*
		 * STEP #2: Evaluate regression model and compute
		 * approved list of metrics
		 */
	    String predictionCol = "_prediction";
	    model.setPredictionCol(predictionCol);

	    Dataset<Row> predictions = model.transform(testset);
	    String modelMetrics = RegressorEvaluator.evaluate(predictions, labelCol, predictionCol);
		/*
		 * STEP #3: Store trained regression model including
		 * its associated parameters and metrics
		 */		
		String modelName = config.modelName;
		String modelStage = config.modelStage;
		
		new IsotonicRecorder(configReader)
				.track(context, modelName, modelStage, modelParams, modelMetrics, model);

	}

	@Override
	public void validateSchema(Schema inputSchema) {
		config.validateSchema(inputSchema);		
	}

	public static class IsotonicConfig extends RegressorConfig {
		  
		private static final long serialVersionUID = -4928234679795163044L;
		
		@Description("This indicator determines whether whether the output sequence should be 'isotonic' (increasing) or 'antitonic' (decreasing). Default is 'isotonic'.")
		@Macro
		public String isotonic;

		@Description("The nonnegative index of the feature. Default is 0.")
		@Macro
		public Integer featureIndex;
		
		public IsotonicConfig() {

			dataSplit = "70:30";
			modelStage = "experiment";
			
			isotonic = "isotonic";
			featureIndex = 0;
			
		}
	    
		@Override
		public Map<String, Object> getParamsAsMap() {
			
			Map<String, Object> params = new HashMap<>();

			params.put("isotonic", isotonic);
			params.put("featureIndex", featureIndex);

			params.put("dataSplit", dataSplit);
			return params;
		
		}

		public void validate() {
			super.validate();

			/* PARAMETERS */
			if (featureIndex < 0)
				throw new IllegalArgumentException(String.format(
						"[%s] The feature index must be nonnegative.", this.getClass().getName()));
			
		}
				
	}

}
