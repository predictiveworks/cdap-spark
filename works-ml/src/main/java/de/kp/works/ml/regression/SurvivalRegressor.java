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

import de.kp.works.core.recording.regression.SurvivalRecorder;
import org.apache.spark.ml.regression.AFTSurvivalRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.google.common.base.Strings;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import io.cdap.cdap.etl.api.batch.SparkSink;

import de.kp.works.core.SchemaUtil;
import de.kp.works.core.recording.RegressorEvaluator;
import de.kp.works.core.regressor.RegressorConfig;
import de.kp.works.core.regressor.RegressorSink;

@Plugin(type = SparkSink.PLUGIN_TYPE)
@Name("SurvivalRegressor")
@Description("A building stage for an Apache Spark ML Survival (AFT) regressor model. This stage expects "
		+ "a dataset with at least two fields to train the model: One as an array of numeric values, and, "  
		+ "another that describes the class or label value as numeric value.")
public class SurvivalRegressor extends RegressorSink {

	private static final long serialVersionUID = -2096945742865221471L;
	
	private final SurvivalConfig config;
	
	public SurvivalRegressor(SurvivalConfig config) {
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

		String censorCol = config.censorCol;
		
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
		AFTSurvivalTrainer trainer = new AFTSurvivalTrainer();
		Dataset<Row> vectorset = trainer.vectorize(source, featuresCol, vectorCol);
		/*
		 * Split the vectorset into a train & test dataset for
		 * later regression evaluation
		 */
	    Dataset<Row>[] splitted = vectorset.randomSplit(config.getSplits());
		
	    Dataset<Row> trainset = splitted[0];
	    Dataset<Row> testset = splitted[1];

	    AFTSurvivalRegressionModel model = trainer.train(trainset, vectorCol, labelCol, censorCol, params);
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
		
		new SurvivalRecorder(configReader)
				.track(context, modelName, modelStage, modelParams, modelMetrics, model);

	}

	@Override
	public void validateSchema(Schema inputSchema) {
		config.validateSchema(inputSchema);		
	}
	
	public static class SurvivalConfig extends RegressorConfig {

		private static final long serialVersionUID = 8618207399826721560L;

		@Description("The name of the field in the input schema that contains the censor value. The censor value "
				+ "can be 0 or 1. If the value is 1, it means the event has occurred (uncensored); otherwise censored.")
		@Macro
		public String censorCol;

		@Description("The maximum number of iterations to train the AFT Survival Regression model. Default is 100.")
		@Macro
		public Integer maxIter;

		@Description("The positive convergence tolerance of iterations. Smaller values will lead to higher accuracy with the cost "
				+ "of more iterations. Default is 1e-6")
		@Macro
		public Double tol;		
		
		/*
		 * Quantiles and associated column are not externalized, i.e. Apache Spark's
		 * default settings are used 
		 */
		
		public SurvivalConfig() {

			dataSplit = "70:30";
			modelStage = "experiment";
			
			maxIter = 100;
			tol = 1e-6;
			
		}
	    
		@Override
		public Map<String, Object> getParamsAsMap() {
			
			Map<String, Object> params = new HashMap<>();

			params.put("maxIter", maxIter);
			params.put("tol", tol);

			params.put("dataSplit", dataSplit);
			return params;
		
		}

		public void validate() {
			super.validate();

			/* MODEL & COLUMNS */
			if (Strings.isNullOrEmpty(censorCol)) {
				throw new IllegalArgumentException(
						String.format("[%s] The name of the field that contains the censor value must not be empty.",
								this.getClass().getName()));
			}
			
			/* PARAMETERS */
			if (maxIter < 1)
				throw new IllegalArgumentException(String.format(
						"[%s] The maximum number of iterations must be at least 1.", this.getClass().getName()));

			if (tol <= 0D)
				throw new IllegalArgumentException(
						String.format("[%s] The iteration tolerance must be positive.", this.getClass().getName()));			
			
		}
		
		public void validateSchema(Schema inputSchema) {
			super.validateSchema(inputSchema);
			
			/* CENSOR COL */
			
			Schema.Field censorField = inputSchema.getField(labelCol);
			if (censorField == null) {
				throw new IllegalArgumentException(String
						.format("[%s] The input schema must contain the field that defines the censor value.",  this.getClass().getName()));		
			}

			Schema.Type censorType = getNonNullIfNullable(censorField.getSchema()).getType();

			if (!SchemaUtil.isNumericType(censorType)) {
				throw new IllegalArgumentException("The data type of the censor field must be numeric.");
			}
			
		}
	}
}
