package de.kp.works.ml.prediction;
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

import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.col;

import com.google.common.base.Strings;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.SparkCompute;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import de.kp.works.core.Params;
import de.kp.works.core.recommender.RecommenderCompute;
import de.kp.works.ml.recommendation.ALSConfig;
import de.kp.works.core.recording.recommendation.ALSRecorder;

@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("ALSPredictor")
@Description("A prediction stage that leverages a trained Apache Spark ML ALS recommendation model.")
public class ALSPredictor extends RecommenderCompute {

	private static final long serialVersionUID = -3668319605054724319L;

	private final ALSPredictorConfig config;
	private ALSModel model;

	public ALSPredictor(ALSPredictorConfig config) {
		this.config = config;
	}

	@Override
	public void initialize(SparkExecutionPluginContext context) throws Exception {
		((ALSConfig) config).validate();

		ALSRecorder recorder = new ALSRecorder(configReader);
		/*
		 * STEP #1: Retrieve the trained recommendation model that refers to the provide
		 * name, stage and option
		 */
		model = recorder.read(context, config.modelName, config.modelStage, config.modelOption);
		if (model == null)
			throw new IllegalArgumentException(
					String.format("[%s] A recommendation model with name '%s' does not exist.",
							this.getClass().getName(), config.modelName));

		/*
		 * STEP #2: Retrieve the profile of the trained recommendation model for
		 * subsequent annotation
		 */
		profile = recorder.getProfile();

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
			validateSchema(inputSchema);
			/*
			 * In cases where the input schema is explicitly provided, we determine the
			 * output schema by explicitly adding the prediction column
			 */
			outputSchema = getOutputSchema(inputSchema, config.predictionCol);
			stageConfigurer.setOutputSchema(outputSchema);

		}

	}

	public void validateSchema(Schema inputSchema) {

		/* USER COLUMN */

		Schema.Field userCol = inputSchema.getField(config.userCol);
		if (userCol == null) {
			throw new IllegalArgumentException(
					String.format("[%s] The input schema must contain the field that defines the user identifier.",
							this.getClass().getName()));
		}

		Schema.Type userType = getNonNullIfNullable(userCol.getSchema()).getType();
		if (!isNumericType(userType)) {
			throw new IllegalArgumentException("The data type of the user field must be NUMERIC.");
		}

		/* ITEM COLUMN */

		Schema.Field itemCol = inputSchema.getField(config.itemCol);
		if (itemCol == null) {
			throw new IllegalArgumentException(
					String.format("[%s] The input schema must contain the field that defines the user identifier.",
							this.getClass().getName()));
		}

		Schema.Type itemType = getNonNullIfNullable(itemCol.getSchema()).getType();
		if (!isNumericType(itemType)) {
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

		model.setUserCol(config.userCol);
		model.setItemCol(config.itemCol);

		model.setPredictionCol(config.predictionCol);
		Dataset<Row> predictions = model.transform(source)
				/*
				 * Apache Spark describes the predicted ratings as Float; to be compliant with
				 * CDAP output schema, we transform into Double
				 */
				.withColumn(config.predictionCol, col(config.predictionCol).cast(DataTypes.DoubleType));

		return annotate(predictions, RECOMMENDER_TYPE);

	}

	public static class ALSPredictorConfig extends ALSConfig {

		private static final long serialVersionUID = -1806437026686955957L;

		@Description("The name of the field in the output schema that contains the predicted rating.")
		@Macro
		public String predictionCol;

		@Description(Params.MODEL_OPTION)
		@Macro
		public String modelOption;

		public ALSPredictorConfig() {
			modelOption = BEST_MODEL;
		}

		public void validate() {
			super.validate();

			if (Strings.isNullOrEmpty(predictionCol))
				throw new IllegalArgumentException(String.format(
						"[%s] The name of the field that contains the predicted rating must not be empty.",
						this.getClass().getName()));

		}

	}
}
