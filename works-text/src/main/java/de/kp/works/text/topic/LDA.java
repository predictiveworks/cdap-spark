package de.kp.works.text.topic;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.ml.clustering.LDAModel;
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
import de.kp.works.core.Params;
import de.kp.works.core.recording.clustering.LDARecorder;
import de.kp.works.core.text.TextCompute;
import de.kp.works.text.recording.Word2VecRecorder;
import de.kp.works.text.embeddings.Word2VecModel;

@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("LDA")
@Description("A transformation stage to map text documents on their topic vectors or most likely topic label. "
		+ "This stage is based on two trained models, an LDA model and a Word Embedding model.")
public class LDA extends TextCompute {

	private static final long serialVersionUID = 5821757318391348559L;

	private final LDAConfig config;

	private LDAModel model;
	private Word2VecModel word2vec;
	
	public LDA(LDAConfig config) {
		this.config = config;
	}

	@Override
	public void initialize(SparkExecutionPluginContext context) throws Exception {
		config.validate();

		model = new LDARecorder(configReader)
				.read(context, config.modelName, config.modelStage, config.modelOption);

		if (model == null)
			throw new IllegalArgumentException(
					String.format("[%s] An LDA model with name '%s' does not exist.",
							this.getClass().getName(), config.modelName));

		/*
		 * Word2Vec models do not have any metrics, i.e. there
		 * is no model option: always the latest model is used
		 */
		word2vec = new Word2VecRecorder(configReader)
				.read(context, config.embeddingName, config.embeddingStage, LATEST_MODEL);

		if (word2vec == null)
			throw new IllegalArgumentException(
					String.format("[%s] A Word2Vec embedding model with name '%s' does not exist.",
							this.getClass().getName(), config.embeddingName));

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
			outputSchema = getOutputSchema(inputSchema);
			stageConfigurer.setOutputSchema(outputSchema);

		}

	}

	/**
	 * A helper method to compute the output schema in that use cases where an input
	 * schema is explicitly given
	 */
	public Schema getOutputSchema(Schema inputSchema) {

		assert inputSchema.getFields() != null;
		List<Schema.Field> fields = new ArrayList<>(inputSchema.getFields());
		
		if (config.topicStrategy.equals("cluster")) {
			fields.add(Schema.Field.of(config.topicCol, Schema.arrayOf(Schema.of(Schema.Type.DOUBLE))));
	    
		} else {
			fields.add(Schema.Field.of(config.topicCol, Schema.of(Schema.Type.DOUBLE)));
		
		}
		return Schema.recordOf(inputSchema.getRecordName() + ".predicted", fields);

	}
	@Override
	public Dataset<Row> compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {
		
		if (config.topicStrategy.equals("cluster")) {

			Map<String, Object> params = config.getParamsAsMap();

			LDAPredictor predictor = new LDAPredictor(model, word2vec);
			Dataset<Row> predictions = predictor.predict(source, config.textCol, config.topicCol, params);
			
			return predictor.devectorize(predictions, config.topicCol, config.topicCol);
			
		}
		else {

			Map<String, Object> params = config.getParamsAsMap();
			
			LDALabeler labeler = new LDALabeler(model, word2vec);
			return labeler.label(source, config.textCol, config.topicCol, params);

		}
	}

	@Override
	public void validateSchema(Schema inputSchema) {

		/* TEXT COLUMN */

		Schema.Field textCol = inputSchema.getField(config.textCol);
		if (textCol == null) {
			throw new IllegalArgumentException(
					String.format("[%s] The input schema must contain the field that defines the text document.",
							this.getClass().getName()));
		}

		isString(config.textCol);

	}

	public static class LDAConfig extends LDATextConfig {

		private static final long serialVersionUID = 3012205481027114331L;

		@Description(Params.MODEL_OPTION)
		@Macro
		public String modelOption;
		
		@Description("The indicator to determine whether the trained LDA model is used to predict a topic label or vector. "
				+ "Supported values are 'label' & 'vector'. Default is 'vector'.")
		@Macro
		public String topicStrategy;

		@Description("The name of the field in the output schema that contains the prediction result.")
		@Macro
		public String topicCol;

		public Map<String, Object> getParamsAsMap() {

			Map<String, Object> params = new HashMap<>();
			params.put("strategy", getStrategy());

			return params;

		}
		
		public LDAConfig() {
			
			modelOption = BEST_MODEL;
			modelStage = "experiment";

			embeddingStage = "experiment";
			
			poolingStrategy = "average";
			topicStrategy = "vector";
		}
		
		public void validate() {
			super.validate();
			
			if (Strings.isNullOrEmpty(topicCol)) {
				throw new IllegalArgumentException(String.format(
						"[%s] The name of the field that contains the prediction result must not be empty.",
						this.getClass().getName()));
			}
			
		}
	}
}
