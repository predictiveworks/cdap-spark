package de.kp.works.text.ner;
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
import com.johnsnowlabs.nlp.annotators.ner.crf.NerCrfModel;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;

import de.kp.works.core.BaseCompute;
import de.kp.works.text.embeddings.Word2VecManager;
import de.kp.works.text.embeddings.Word2VecModel;

@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("NERTagger")
@Description("An tagging stage that leverages a trained Word2Vec model and NER (CRF) model to map an input "
		+ "text field onto an output token & entities field.")
public class NERTagger extends BaseCompute {

	private static final long serialVersionUID = -7309094976122977799L;

	private NERTaggerConfig config;

	private NerCrfModel model;
	private Word2VecModel word2vec;
	
	public NERTagger(NERTaggerConfig config) {
		this.config = config;
	}

	@Override
	public void initialize(SparkExecutionPluginContext context) throws Exception {
		config.validate();

		model = new NERManager().read(modelFs, modelMeta, config.modelName);
		if (model == null)
			throw new IllegalArgumentException(
					String.format("[%s] A NER (CRF) model with name '%s' does not exist.",
							this.getClass().getName(), config.modelName));

		word2vec = new Word2VecManager().read(modelFs, modelMeta, config.embeddingName);
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
			validateSchema(inputSchema, config);
			/*
			 * In cases where the input schema is explicitly provided, we determine the
			 * output schema by explicitly adding the prediction column
			 */
			outputSchema = getOutputSchema(inputSchema,config.tokenCol, config.nerCol);
			stageConfigurer.setOutputSchema(outputSchema);

		}

	}
	
	@Override
	public Dataset<Row> compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {

		NERPredictor predictor = new NERPredictor(model, word2vec);
		return predictor.predict(source, config.textCol, config.tokenCol, config.nerCol);
		
	}

	/**
	 * A helper method to compute the output schema in that use cases where an input
	 * schema is explicitly given
	 */
	public Schema getOutputSchema(Schema inputSchema, String tokenField, String nerField) {

		List<Schema.Field> fields = new ArrayList<>(inputSchema.getFields());
		
		fields.add(Schema.Field.of(tokenField, Schema.arrayOf(Schema.of(Schema.Type.STRING))));
		fields.add(Schema.Field.of(nerField, Schema.arrayOf(Schema.arrayOf(Schema.of(Schema.Type.FLOAT)))));
		
		return Schema.recordOf(inputSchema.getRecordName() + ".tagged", fields);

	}

	public void validateSchema(Schema inputSchema, NERTaggerConfig config) {

		/** TEXT COLUMN **/

		Schema.Field textCol = inputSchema.getField(config.textCol);
		if (textCol == null) {
			throw new IllegalArgumentException(
					String.format("[%s] The input schema must contain the field that defines the text document.",
							this.getClass().getName()));
		}

		isString(config.textCol);

	}
	
	public static class NERTaggerConfig extends BaseNERConfig {

		private static final long serialVersionUID = -1825371412290503006L;

		@Description("The name of the field in the output schema that contains the extracted tokens.")
		@Macro
		public String tokenCol;

		@Description("The name of the field in the output schema that contains the extracted entities.")
		@Macro
		public String nerCol;

		public void validate() {
			super.validate();
			
			if (Strings.isNullOrEmpty(tokenCol)) {
				throw new IllegalArgumentException(String.format(
						"[%s] The name of the field that contains the extracted tokens must not be empty.",
						this.getClass().getName()));
			}
			
			if (Strings.isNullOrEmpty(nerCol)) {
				throw new IllegalArgumentException(String.format(
						"[%s] The name of the field that contains the extracted entities must not be empty.",
						this.getClass().getName()));
			}
			
		}
		
	}
}
