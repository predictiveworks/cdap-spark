package de.kp.works.text.dep;
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

import de.kp.works.text.recording.DependencyRecorder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.google.common.base.Strings;
import com.johnsnowlabs.nlp.annotators.parser.dep.DependencyParserModel;
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
import de.kp.works.text.recording.POSRecorder;
import de.kp.works.text.util.Names;

@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("DependencyParser")
@Description("A transformation stage that leverages an Unlabeled Dependency Parser model to extract "
		+ "syntactic relations between words in a text document.")
public class DependencyParser extends TextCompute {

	private static final long serialVersionUID = 2556276054866498202L;

	private final DependencyParserConfig config;
	
	private DependencyParserModel model;
	private PerceptronModel perceptron;
	
	public DependencyParser(DependencyParserConfig config) {
		this.config = config;
	}

	@Override
	public void initialize(SparkExecutionPluginContext context) throws Exception {
		config.validate();
		/*
		 * Dependency parser models do not have any metrics, i.e. there
		 * is no model option: always the latest model is used
		 */
		model = new DependencyRecorder(configReader)
				.read(context, config.modelName, config.modelStage, LATEST_MODEL);

		if (model == null)
			throw new IllegalArgumentException(
					String.format("[%s] A Dependency Parser model with name '%s' does not exist.",
							this.getClass().getName(), config.modelName));

		/*
		 * Part-of-Speech models do not have any metrics, i.e. there
		 * is no model option: always the latest model is used
		 */
		perceptron = new POSRecorder(configReader)
				.read(context, config.posName, config.posStage, LATEST_MODEL);

		if (perceptron == null)
			throw new IllegalArgumentException(
					String.format("[%s] A Part-of-Speech model with name '%s' does not exist.",
							this.getClass().getName(), config.posName));

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
			outputSchema = getOutputSchema(inputSchema,config.sentenceCol, config.dependencyCol);
			stageConfigurer.setOutputSchema(outputSchema);

		}

	}
	
	@Override
	public Dataset<Row> compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {

		DepPredictor predictor = new DepPredictor(model, perceptron);
		return predictor.predict(source, config.textCol, config.sentenceCol, config.dependencyCol);
		
	}
	@Override
	public void validateSchema(Schema inputSchema) {

		/*  TEXT COLUMN */

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
	public Schema getOutputSchema(Schema inputSchema, String sentenceField, String dependencyField) {

		assert inputSchema.getFields() != null;
		List<Schema.Field> fields = new ArrayList<>(inputSchema.getFields());
		
		fields.add(Schema.Field.of(sentenceField, Schema.arrayOf(Schema.of(Schema.Type.STRING))));
		fields.add(Schema.Field.of(dependencyField, Schema.arrayOf(Schema.of(Schema.Type.STRING))));
		
		return Schema.recordOf(inputSchema.getRecordName() + ".parsed", fields);

	}

	public static class DependencyParserConfig extends ModelConfig {

		private static final long serialVersionUID = 4755283827244455912L;

		@Description("The unique name of trained Part of Speech model.")
		@Macro
		public String posName;

		@Description("The stage of the Part of Speech model. Supported values are 'experiment', "
				+ "'stagging', 'production' and 'archived'. Default is 'experiment'.")
		@Macro
		public String posStage;

		@Description(Names.TEXT_COL)
		@Macro
		public String textCol;

		@Description(Names.SENTENCE_COL)
		@Macro
		public String sentenceCol;

		@Description("The name of the field in the output schema that contains the word dependencies.")
		@Macro
		public String dependencyCol;
		
		public DependencyParserConfig() {
			
			modelStage = "experiment";
			posStage = "experiment";
			
		}
		
		public void validate() {
			super.validate();
			
			if (Strings.isNullOrEmpty(posName)) {
				throw new IllegalArgumentException(String.format(
						"[%s] The name of the trained Part-of-Speech model must not be empty.",
						this.getClass().getName()));
			}
			
			if (Strings.isNullOrEmpty(textCol)) {
				throw new IllegalArgumentException(String.format(
						"[%s] The name of the field that contains the text document must not be empty.",
						this.getClass().getName()));
			}
			
			if (Strings.isNullOrEmpty(sentenceCol)) {
				throw new IllegalArgumentException(String.format(
						"[%s] The name of the field that contains the extracted sentences must not be empty.",
						this.getClass().getName()));
			}
			
			if (Strings.isNullOrEmpty(dependencyCol)) {
				throw new IllegalArgumentException(String.format(
						"[%s] The name of the field that contains the word dependencies must not be empty.",
						this.getClass().getName()));
			}			
			
		}
	}
}
