package de.kp.works.text.pos;
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
import com.johnsnowlabs.nlp.annotators.pos.perceptron.PerceptronModel;

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
import de.kp.works.text.config.ModelConfig;
import de.kp.works.text.pos.Chunker;
import de.kp.works.text.util.Names;

@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("POSChunker")
@Description("A transformation stage that extracts meaningful phrases from text documents. Phrase extraction "
		+ "is based on patterns of part-of-speech tags. This stage requires a trained Part-of-Speech model.")
public class POSChunker extends TextCompute {
	
	private static final long serialVersionUID = 4211653733506144147L;

	private POSChunkerConfig config;
	private PerceptronModel model;

	public POSChunker(POSChunkerConfig config) {
		this.config = config;
	}

	@Override
	public void initialize(SparkExecutionPluginContext context) throws Exception {
		config.validate();

		/*
		 * Part-of-Speech models do not have any metrics, i.e. there
		 * is no model option: always the latest model is used
		 */
		model = new POSRecorder().read(context, config.modelName, config.modelStage, LATEST_MODEL);
		if (model == null)
			throw new IllegalArgumentException(
					String.format("[%s] A Part-of-Speech analysis model with name '%s' does not exist.",
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
			 * output schema by explicitly adding the token & chunk columns
			 */
			outputSchema = getOutputSchema(inputSchema);
			stageConfigurer.setOutputSchema(outputSchema);

		}

	}
	/**
	 * This method computes chunks by applying a trained Part-of-Speech model; as a result,
	 * the source dataset is enriched by two extra columns of data type Array[String]
	 */
	@Override
	public Dataset<Row> compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {

		Chunker chunker = new Chunker(model);
		return chunker.chunk(source, config.getRules(), config.textCol, config.chunkCol);
		
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
	protected Schema getOutputSchema(Schema inputSchema) {

		List<Schema.Field> fields = new ArrayList<>(inputSchema.getFields());
		fields.add(Schema.Field.of(config.chunkCol, Schema.arrayOf(Schema.of(Schema.Type.STRING))));
		
		return Schema.recordOf(inputSchema.getRecordName() + ".transformed", fields);

	}

	public static class POSChunkerConfig extends ModelConfig {

		private static final long serialVersionUID = -7335693906960966678L;

		@Description(Names.TEXT_COL)
		@Macro
		public String textCol;

		@Description("The name of the field in the output schema that contains the extracted chunks.")
		@Macro
		public String chunkCol;

		@Description("A delimiter separated list of chunking rules.")
		@Macro
		private String rules;

		@Description("The delimiter used to separate the different chunking rules.")
		@Macro
		private String delimiter;
		
		public POSChunkerConfig() {
			modelStage = "experiment";
			delimiter = ",";
		}

		public List<String> getRules() {
			
			String cleaned = rules.replaceAll("\\r\\n|\\r|\\n", " ");
			List<String> parsers = new ArrayList<>();
			
			for (String token: cleaned.split(delimiter)) {
				parsers.add(token.trim());
			}
			
			return parsers;
			
		}

		public void validate() {
			super.validate();
			
			if (Strings.isNullOrEmpty(textCol)) {
				throw new IllegalArgumentException(String.format(
						"[%s] The name of the field that contains the text document must not be empty.",
						this.getClass().getName()));
			}
			
			if (Strings.isNullOrEmpty(chunkCol)) {
				throw new IllegalArgumentException(String.format(
						"[%s] The name of the field that contains the extracted chunks must not be empty.",
						this.getClass().getName()));
			}

			if (Strings.isNullOrEmpty(rules)) {
				throw new IllegalArgumentException(
						String.format("[%s] The chunking rules must not be empty.", this.getClass().getName()));
			}

			if (Strings.isNullOrEmpty(delimiter)) {
				throw new IllegalArgumentException(
						String.format("[%s] The rule delimiter must not be empty.", this.getClass().getName()));
			}
			
		}
		
	}
}
