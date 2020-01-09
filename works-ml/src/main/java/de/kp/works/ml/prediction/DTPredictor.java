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

import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
import org.apache.spark.ml.regression.DecisionTreeRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.google.common.base.Strings;

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
import de.kp.works.ml.classification.DTClassifierManager;
import de.kp.works.ml.regression.DTRegressorManager;

@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("DTPredictor")
@Description("A prediction stage that leverages a trained Apache Spark based Decision Tree classifier or regressor model.")
public class DTPredictor extends BasePredictorCompute {

	private static final long serialVersionUID = 4611875710426366606L;

	private DTPredictorConfig config;

	private DecisionTreeClassificationModel classifier;
	private DecisionTreeRegressionModel regressor;

	public DTPredictor(DTPredictorConfig config) {
		this.config = config;
	}

	@Override
	public void initialize(SparkExecutionPluginContext context) throws Exception {
		config.validate();

		if (config.modelType.equals("classifier")) {

			modelFs = SparkMLManager.getClassificationFS(context);
			modelMeta = SparkMLManager.getClassificationMeta(context);

			classifier = new DTClassifierManager().read(modelFs, modelMeta, config.modelName);
			if (classifier == null)
				throw new IllegalArgumentException(String
						.format("[%s] A classifier model with name '%s' does not exist.", this.getClass().getName(), config.modelName));

		} else if (config.modelType.equals("regressor")) {

			modelFs = SparkMLManager.getRegressionFS(context);
			modelMeta = SparkMLManager.getRegressionMeta(context);

			regressor = new DTRegressorManager().read(modelFs, modelMeta, config.modelName);
			if (regressor == null)
				throw new IllegalArgumentException(String
						.format("[%s] A regressor model with name '%s' does not exist.", this.getClass().getName(), config.modelName));

		} else
			throw new IllegalArgumentException(
					String.format("[%s] The model type '%s' is not supported.", this.getClass().getName(), config.modelType));

	}

	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {

		config.validate();

		StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
		/*
		 * Try to determine input and output schema; if these schemas are not explicitly
		 * specified, they will be inferred from the provided data records
		 */
		inputSchema = stageConfigurer.getInputSchema();
		if (inputSchema != null) {
			/*
			 * In cases where the input schema is explicitly provided, we determine the
			 * output schema by explicitly adding the prediction column
			 */
			outputSchema = getOutputSchema(inputSchema, config.predictionCol);
			stageConfigurer.setOutputSchema(outputSchema);

		}

	}

	/**
	 * This method computes predictions either by applying a trained Decision Tree
	 * classification or regression model; as a result, the source dataset is
	 * enriched by an extra column (predictionCol) that specifies the target
	 * variable in form of a Double value
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
		 * as Array[Double]
		 */
		Dataset<Row> vectorset = MLUtils.vectorize(source, featuresCol, vectorCol);
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

	public static class DTPredictorConfig extends BasePredictorConfig {

		private static final long serialVersionUID = 7210199521231877169L;

		public void validate() {

			/** MODEL & COLUMNS **/
			if (!Strings.isNullOrEmpty(modelName)) {
				throw new IllegalArgumentException(
						String.format("[%s] The model name must not be empty.", this.getClass().getName()));
			}
			if (!Strings.isNullOrEmpty(featuresCol)) {
				throw new IllegalArgumentException(
						String.format("[%s] The name of the field that contains the feature vector must not be empty.",
								this.getClass().getName()));
			}
			if (!Strings.isNullOrEmpty(predictionCol)) {
				throw new IllegalArgumentException(String.format(
						"[%s] The name of the field that contains the predicted label value must not be empty.",
						this.getClass().getName()));
			}

		}
	}
}