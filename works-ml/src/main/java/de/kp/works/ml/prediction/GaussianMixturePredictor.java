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

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.ml.clustering.GaussianMixtureModel;
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
import io.cdap.cdap.etl.api.batch.SparkCompute;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;

import de.kp.works.core.cluster.PredictorConfig;
import de.kp.works.core.predictor.PredictorCompute;

import de.kp.works.core.recording.MLUtils;
import de.kp.works.core.recording.clustering.GaussianMixtureRecorder;

@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("GaussianMixturePredictor")
@Description("A prediction stage that leverages a trained Apache Spark ML Gaussian Mixture clustering model.")
public class GaussianMixturePredictor extends PredictorCompute {

	private static final long serialVersionUID = 2048099898896242709L;

	private final GaussianMixturePredictorConfig config;
	private GaussianMixtureModel model;

	public GaussianMixturePredictor(GaussianMixturePredictorConfig config) {
		this.config = config;
	}

	@Override
	public void initialize(SparkExecutionPluginContext context) throws Exception {
		config.validate();

		GaussianMixtureRecorder recorder = new GaussianMixtureRecorder();
		/* 
		 * STEP #1: Retrieve the trained clustering model
		 * that refers to the provide name, stage and option
		 */
		model = recorder.read(context, config.modelName, config.modelStage, config.modelOption);
		if (model == null)
			throw new IllegalArgumentException(String.format("[%s] A clustering model with name '%s' does not exist.",
					this.getClass().getName(), config.modelName));

		/* 
		 * STEP #2: Retrieve the profile of the trained
		 * clustering model for subsequent annotation
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
			 * In cases where the input schema is explicitly provided, we determine 
			 * the output schema by explicitly adding the prediction & probability 
			 * column; also the annotation column is added
			 */
			outputSchema = getOutputSchema(inputSchema, config.predictionCol, config.probabilityCol);
			stageConfigurer.setOutputSchema(outputSchema);

		}

	}

	public Schema getOutputSchema(Schema inputSchema, String predictionField, String probabilityField) {

		assert inputSchema.getFields() != null;
		List<Schema.Field> fields = new ArrayList<>(inputSchema.getFields());
		
		fields.add(Schema.Field.of(predictionField, Schema.of(Schema.Type.DOUBLE)));
		fields.add(Schema.Field.of(probabilityField, Schema.arrayOf(Schema.of(Schema.Type.DOUBLE))));		
		/* 
		 * Check whether the input schema already has an 
		 * annotation field defined; the predictor stage
		 * may not be the first stage that annotates model
		 * specific metadata 
		 */
		if (inputSchema.getField(ANNOTATION_COL) == null)
			fields.add(Schema.Field.of(ANNOTATION_COL, Schema.of(Schema.Type.STRING)));

		return Schema.recordOf(inputSchema.getRecordName() + ".predicted", fields);

	}
	/**
	 * This method computes predictions either by applying a trained Gaussian 
	 * Mixture clustering model; as a result, the source dataset is enriched by
	 * an extra column (predictionCol) that specifies the target variable in 
	 * form of a Double value, and by another column (probabilityCol) that holds
	 * the probabilities for each cluster
	 */
	@Override
	public Dataset<Row> compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {
		/*
		 * STEP #1: Extract configuration parameters
		 */
		String featuresCol = config.featuresCol;
		String predictionCol = config.predictionCol;
		String probabilityCol = config.probabilityCol;
		/*
		 * The vectorCol specifies the internal column that has to be built from the
		 * featuresCol and that is used for prediction purposes
		 */
		String vectorCol = "_vector";
		/*
		 * Prepare provided dataset by vectorizing the feature column which
		 * is specified as Array[Numeric]; this adds a _vector column to the
		 * dataset.
		 */
		Dataset<Row> vectorset = MLUtils.vectorize(source, featuresCol, vectorCol, true);

		model.setFeaturesCol(vectorCol);
		model.setPredictionCol(predictionCol);

		model.setProbabilityCol("_probability");
		/*
		 * The probability column is a vector and must be transformed into
		 * an Array[Numeric]
		 */
		Dataset<Row> predictions = MLUtils.devectorize(
				model.transform(vectorset), "_probability", probabilityCol);
		/*
		 * Remove intermediate vector column from predictions
		 * and annotate each prediction with the model profile
		 */
		Dataset<Row> output = predictions.drop(vectorCol);
		return annotate(output, CLUSTER_TYPE);

	}

	@Override
	public void validateSchema(Schema inputSchema) {
		config.validateSchema(inputSchema);
	}

	public static class GaussianMixturePredictorConfig extends PredictorConfig {

		private static final long serialVersionUID = 4186952265159732046L;

		@Description("The name of the field in the output schema that contains the probability vector, i.e. the probability for each cluster.")
		@Macro
		public String probabilityCol;

		public GaussianMixturePredictorConfig() {
			super();
		}

		public void validate() {
			super.validate();

			if (Strings.isNullOrEmpty(probabilityCol)) {
				throw new IllegalArgumentException(
						String.format("[%s] The name of the field that contains the probability vector must not be empty.", this.getClass().getName()));
			}

		}
	}

}
