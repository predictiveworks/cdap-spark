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

import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.col;

import com.google.common.base.Strings;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import de.kp.works.core.RecommenderCompute;
import de.kp.works.core.RecommenderConfig;
import de.kp.works.ml.recommendation.ALSConfig;
import de.kp.works.ml.recommendation.ALSManager;
import de.kp.works.ml.recommendation.ALSSink.ALSSinkConfig;

@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("ALSPredictor")
@Description("A prediction stage that leverages a trained Apache Spark based Alternating Least Squares recommendation model.")
public class ALSPredictor extends RecommenderCompute {

	private static final long serialVersionUID = -3668319605054724319L;

	private ALSPredictorConfig config;
	private ALSModel model;

	public ALSPredictor(ALSPredictorConfig config) {
		this.config = config;
	}

	@Override
	public void initialize(SparkExecutionPluginContext context) throws Exception {
		((ALSConfig) config).validate();

		model = new ALSManager().read(modelFs, modelMeta, config.modelName);
		if (model == null)
			throw new IllegalArgumentException(
					String.format("[%s] A recommendation model with name '%s' does not exist.",
							this.getClass().getName(), config.modelName));

	}

	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {

		((ALSPredictorConfig) config).validate();

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

	@Override
	protected void validateSchema(Schema inputSchema, RecommenderConfig config) {

		ALSSinkConfig alsConfig = (ALSSinkConfig) config;

		/** USER COLUMN **/

		Schema.Field userCol = inputSchema.getField(alsConfig.userCol);
		if (userCol == null) {
			throw new IllegalArgumentException(
					String.format("[%s] The input schema must contain the field that defines the user identifier.",
							this.getClass().getName()));
		}

		Schema.Type userType = userCol.getSchema().getType();
		if (isNumericType(userType) == false) {
			throw new IllegalArgumentException("The data type of the user field must be NUMERIC.");
		}

		/** ITEM COLUMN **/

		Schema.Field itemCol = inputSchema.getField(alsConfig.itemCol);
		if (itemCol == null) {
			throw new IllegalArgumentException(
					String.format("[%s] The input schema must contain the field that defines the user identifier.",
							this.getClass().getName()));
		}

		Schema.Type itemType = itemCol.getSchema().getType();
		if (isNumericType(itemType) == false) {
			throw new IllegalArgumentException("The data type of the item field must be NUMERIC.");
		}

	}

	/**
	 * This method computes predictions either by applying a trained Alternating
	 * Least Squares recommendation model; as a result, the source dataset is
	 * enriched by an extra column (predictionCol) that specifies the target
	 * variable in form of a Double value
	 */
	@Override
	public Dataset<Row> compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {

		ALSPredictorConfig predictorConfig = (ALSPredictorConfig) config;

		model.setUserCol(predictorConfig.userCol);
		model.setItemCol(predictorConfig.itemCol);

		model.setPredictionCol(predictorConfig.predictionCol);
		Dataset<Row> predictions = model.transform(source);
		/*
		 * Apache Spark describes the predicted ratings as Float; to be compliant with
		 * CDAP output schema, we transform into Double
		 */
		return predictions.withColumn(predictorConfig.predictionCol,
				col(predictorConfig.predictionCol).cast(DataTypes.DoubleType));

	}

	public static class ALSPredictorConfig extends ALSConfig {

		private static final long serialVersionUID = -1806437026686955957L;

		@Description("The name of the field in the output schema that contains the predicted rating.")
		@Macro
		public String predictionCol;

		public void validate() {
			super.validate();

			if (Strings.isNullOrEmpty(predictionCol))
				throw new IllegalArgumentException(String.format(
						"[%s] The name of the field that contains the predicted rating must not be empty.",
						this.getClass().getName()));

		}

	}
}
