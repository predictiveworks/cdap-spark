package de.kp.works.text.embeddings;
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

import de.kp.works.text.recording.Word2VecRecorder;
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

import de.kp.works.core.text.TextCompute;
import de.kp.works.text.util.Names;

@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("Word2Vec")
@Description("An embedding stage that leverages a trained Word2Vec model to map an input "
		+ "text field onto an output token & word embedding field.")
public class Word2Vec extends TextCompute {

	private static final long serialVersionUID = 1849637381439375304L;

	private final Word2VecConfig config;
	private Word2VecModel model;

	public Word2Vec(Word2VecConfig config) {
		this.config = config;
	}

	@Override
	public void initialize(SparkExecutionPluginContext context) throws Exception {
		config.validate();

		/*
		 * Word2Vec models do not have any metrics, i.e. there
		 * is no model option: always the latest model is used
		 */
		model = new Word2VecRecorder().read(context, config.modelName, config.modelStage, LATEST_MODEL);
		if (model == null)
			throw new IllegalArgumentException(
					String.format("[%s] A Word2Vec embedding model with name '%s' does not exist.",
							this.getClass().getName(), config.modelName));

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
			outputSchema = getOutputSchema(inputSchema, config.tokenCol, config.embeddingCol);
			stageConfigurer.setOutputSchema(outputSchema);

		}

	}

	@Override
	public Dataset<Row> compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {

		Word2VecEmbedder embedder = new Word2VecEmbedder(model);
		return embedder.embed(source, config.textCol, config.tokenCol, config.embeddingCol, config.getNormalization());

	}

	/**
	 * A helper method to compute the output schema in that use cases where an input
	 * schema is explicitly given
	 */
	public Schema getOutputSchema(Schema inputSchema, String tokenField, String embeddingField) {

		assert inputSchema.getFields() != null;
		List<Schema.Field> fields = new ArrayList<>(inputSchema.getFields());

		fields.add(Schema.Field.of(tokenField, Schema.arrayOf(Schema.of(Schema.Type.STRING))));
		fields.add(Schema.Field.of(embeddingField, Schema.arrayOf(Schema.arrayOf(Schema.of(Schema.Type.FLOAT)))));

		return Schema.recordOf(inputSchema.getRecordName() + ".transformed", fields);

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

	public static class Word2VecConfig extends BaseWord2VecConfig {

		private static final long serialVersionUID = 1874465715678995387L;

		@Description(Names.TOKEN_COL)
		@Macro
		public String tokenCol;

		@Description("The name of the field in the output schema that contains the word embeddings.")
		@Macro
		public String embeddingCol;

		public Word2VecConfig() {
			modelStage = "experiment";
		}

		public void validate() {
			super.validate();

			if (Strings.isNullOrEmpty(tokenCol)) {
				throw new IllegalArgumentException(String.format(
						"[%s] The name of the field that contains the extracted tokens must not be empty.",
						this.getClass().getName()));
			}

			if (Strings.isNullOrEmpty(embeddingCol)) {
				throw new IllegalArgumentException(
						String.format("[%s] The name of the field that contains the word embeddings must not be empty.",
								this.getClass().getName()));
			}

		}
	}
}
