package de.kp.works.ml.prediction;
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

import org.apache.spark.ml.classification.GBTClassificationModel;
import org.apache.spark.ml.regression.GBTRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import de.kp.works.core.BasePredictorCompute;
import de.kp.works.core.BasePredictorConfig;
import de.kp.works.core.ml.SparkMLManager;
import de.kp.works.ml.MLUtils;
import de.kp.works.ml.classification.GBTClassifierManager;
import de.kp.works.ml.regression.GBTRegressorManager;

@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("GBTPredictor")
@Description("A prediction stage that leverages a trained Apache Spark based Gradient-Boosted Trees classifier or regressor model.")
public class GBTPredictor extends BasePredictorCompute {

	private static final long serialVersionUID = 4445941695722336690L;

	private GBTPredictorConfig config;

	private GBTClassificationModel classifier;
	private GBTRegressionModel regressor;

	public GBTPredictor(GBTPredictorConfig config) {
		this.config = config;
	}

	@Override
	public void initialize(SparkExecutionPluginContext context) throws Exception {
		((GBTPredictorConfig)config).validate();

		if (config.modelType.equals("classifier")) {

			modelFs = SparkMLManager.getClassificationFS(context);
			modelMeta = SparkMLManager.getClassificationMeta(context);

			classifier = new GBTClassifierManager().read(modelFs, modelMeta, config.modelName);
			if (classifier == null)
				throw new IllegalArgumentException(String
						.format("[%s] A classifier model with name '%s' does not exist.", this.getClass().getName(), config.modelName));

		} else if (config.modelType.equals("regressor")) {

			modelFs = SparkMLManager.getRegressionFS(context);
			modelMeta = SparkMLManager.getRegressionMeta(context);

			regressor = new GBTRegressorManager().read(modelFs, modelMeta, config.modelName);
			if (regressor == null)
				throw new IllegalArgumentException(String
						.format("[%s] A regressor model with name '%s' does not exist.", this.getClass().getName(), config.modelName));

		} else
			throw new IllegalArgumentException(
					String.format("[%s] The model type '%s' is not supported.", this.getClass().getName(), config.modelType));

	}

	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {

		((GBTPredictorConfig)config).validate();

		StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
		/*
		 * Try to determine input and output schema; if these schemas are not explicitly
		 * specified, they will be inferred from the provided data records
		 */
		inputSchema = stageConfigurer.getInputSchema();
		if (inputSchema != null) {
			validateSchema(inputSchema, config);
			/*
			 * In cases where the input schema is explicitly provided, we determine the
			 * output schema by explicitly adding the prediction column
			 */
			outputSchema = getOutputSchema(inputSchema, config.predictionCol);
			stageConfigurer.setOutputSchema(outputSchema);

		}

	}

	/**
	 * This method computes predictions either by applying a trained Gradient
	 * Boosted Trees classification or regression model; as a result, the source
	 * dataset is enriched by an extra column (predictionCol) that specifies the 
	 * target variable in form of a Double value
	 */
	@Override
	public Dataset<Row> compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {
		/*
		 * STEP #1: Extract configuration parameters
		 */
		String featuresCol = config.featuresCol;
		String predictionCol = config.predictionCol;
		/*
		 * The vectorCol specifies the internal column that has to be built from the
		 * featuresCol and that is used for prediction purposes
		 */
		String vectorCol = "_vector";
		/*
		 * Prepare provided dataset by vectorizing the feature column which is specified
		 * as Array[Numeric]
		 */
		Dataset<Row> vectorset = MLUtils.vectorize(source, featuresCol, vectorCol, true);
		Dataset<Row> predictions = null;

		if (config.modelType.equals("classifier")) {

			classifier.setFeaturesCol(vectorCol);
			classifier.setPredictionCol(predictionCol);

			predictions = classifier.transform(vectorset);

		} else {

			regressor.setFeaturesCol(vectorCol);
			regressor.setPredictionCol(predictionCol);

			predictions = regressor.transform(vectorset);

		}

		Dataset<Row> output = predictions.drop(vectorCol);
		return output;

	}

	public static class GBTPredictorConfig extends BasePredictorConfig {

		private static final long serialVersionUID = 8253356507092880481L;

		public void validate() {
			super.validate();

		}
	}

}
