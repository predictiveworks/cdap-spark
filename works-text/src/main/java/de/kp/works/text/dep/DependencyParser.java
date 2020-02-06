package de.kp.works.text.dep;
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
import com.johnsnowlabs.nlp.annotators.parser.dep.DependencyParserModel;
import com.johnsnowlabs.nlp.annotators.pos.perceptron.PerceptronModel;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;

import de.kp.works.core.BaseCompute;
import de.kp.works.text.pos.POSManager;

public class DependencyParser extends BaseCompute {

	private static final long serialVersionUID = 2556276054866498202L;

	private DependencyParserConfig config;
	
	private DependencyParserModel model;
	private PerceptronModel perceptron;
	
	public DependencyParser(DependencyParserConfig config) {
		this.config = config;
	}

	@Override
	public void initialize(SparkExecutionPluginContext context) throws Exception {
		config.validate();

		model = new DependencyManager().read(context, config.modelName);
		if (model == null)
			throw new IllegalArgumentException(
					String.format("[%s] A Dependency Parser model with name '%s' does not exist.",
							this.getClass().getName(), config.modelName));

		perceptron = new POSManager().read(context, config.posName);
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
	public Schema getOutputSchema(Schema inputSchema, String sentenceField, String dependencyField) {

		List<Schema.Field> fields = new ArrayList<>(inputSchema.getFields());
		
		fields.add(Schema.Field.of(sentenceField, Schema.arrayOf(Schema.of(Schema.Type.STRING))));
		fields.add(Schema.Field.of(dependencyField, Schema.arrayOf(Schema.of(Schema.Type.STRING))));
		
		return Schema.recordOf(inputSchema.getRecordName() + ".parsed", fields);

	}

	public static class DependencyParserConfig extends BaseDependencyConfig {

		private static final long serialVersionUID = 4755283827244455912L;

		@Description("The unique name of trained Part-of-Speech model.")
		@Macro
		public String posName;

		@Description("The name of the field in the input schema that contains the text document.")
		@Macro
		public String textCol;

		@Description("The name of the field in the output schema that contains the extracted sentences.")
		@Macro
		public String sentenceCol;

		@Description("The name of the field in the output schema that contains the word dependencies.")
		@Macro
		public String dependencyCol;
		
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
