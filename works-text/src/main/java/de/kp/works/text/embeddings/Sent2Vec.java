package de.kp.works.text.embeddings;
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

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

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

import de.kp.works.core.text.TextCompute;
import de.kp.works.text.embeddings.Word2VecModel;
import de.kp.works.text.util.Names;

@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("Sent2Vec")
@Description("An embedding stage that leverages a trained Word2Vec model to map an input "
		+ "text field onto an output sentence & sentence embedding field with a user-specific pooling strategy.")
public class Sent2Vec extends TextCompute {

	private static final long serialVersionUID = 2454938187092703188L;

	private Sent2VecConfig config;
	private Word2VecModel model;

	@Override
	public void initialize(SparkExecutionPluginContext context) throws Exception {
		config.validate();

		model = new Word2VecRecorder().read(context, config.modelName, config.modelStage);
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
			outputSchema = getOutputSchema(inputSchema, config.sentenceCol, config.embeddingCol);
			stageConfigurer.setOutputSchema(outputSchema);

		}

	}

	@Override
	public Dataset<Row> compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {

		Sent2VecEmbedder embedder = new Sent2VecEmbedder(model);
		return embedder.embed(source, config.getStrategy(), config.textCol, config.sentenceCol, config.embeddingCol,
				config.getNormalization());

	}

	@Override
	public void validateSchema(Schema inputSchema) {

		/** TEXT COLUMN **/

		Schema.Field textCol = inputSchema.getField(config.textCol);
		if (textCol == null) {
			throw new IllegalArgumentException(
					String.format("[%s] The input schema must contain the field that defines the text document.",
							this.getClass().getName()));
		}

		isString(config.textCol);

	}

	/**
	 * A helper method to compute the output schema in that use cases where an input
	 * schema is explicitly given
	 */
	public Schema getOutputSchema(Schema inputSchema, String sentenceField, String embeddingField) {

		List<Schema.Field> fields = new ArrayList<>(inputSchema.getFields());

		fields.add(Schema.Field.of(sentenceField, Schema.arrayOf(Schema.of(Schema.Type.STRING))));
		fields.add(Schema.Field.of(embeddingField, Schema.arrayOf(Schema.arrayOf(Schema.of(Schema.Type.FLOAT)))));

		return Schema.recordOf(inputSchema.getRecordName() + ".transformed", fields);

	}

	public static class Sent2VecConfig extends BaseWord2VecConfig {

		private static final long serialVersionUID = 5849792304953202359L;

		@Description(Names.SENTENCE_COL)
		@Macro
		public String sentenceCol;

		@Description("The name of the field in the output schema that contains the sentence embeddings.")
		@Macro
		public String embeddingCol;

		@Description(Names.POOLING_STRATEGY)
		@Macro
		public String poolingStrategy;

		public Sent2VecConfig() {
			
			modelStage = "experiment";
			
			normalization = "true";
			poolingStrategy = "average";

		}

		public String getStrategy() {
			return (poolingStrategy.equals("average")) ? "AVERAGE" : "SUM";
		}

		public void validate() {
			super.validate();

			if (Strings.isNullOrEmpty(sentenceCol)) {
				throw new IllegalArgumentException(String.format(
						"[%s] The name of the field that contains the extracted sentences must not be empty.",
						this.getClass().getName()));
			}

			if (Strings.isNullOrEmpty(embeddingCol)) {
				throw new IllegalArgumentException(String.format(
						"[%s] The name of the field that contains the sentence embeddings must not be empty.",
						this.getClass().getName()));
			}

			if (Strings.isNullOrEmpty(poolingStrategy)) {
				throw new IllegalArgumentException(String.format(
						"[%s] The name of the field that contains the pooling strategy must not be empty.",
						this.getClass().getName()));
			}

		}
	}
}
