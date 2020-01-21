package de.kp.works.text;
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
import java.util.Properties;

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
import de.kp.works.core.BaseCompute;
import de.kp.works.core.BaseConfig;

@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("SentenceTokenizer")
@Description("A transformation stage that leverages the Spark NLP Tokenizer to map an input "
		+ "text field with sentence annotations into an output field that contains detected sentence tokens.")
public class SentenceTokenizer extends BaseCompute {

	private static final long serialVersionUID = 1L;

	private SentenceTokenizerConfig config;
	
	public SentenceTokenizer(SentenceTokenizerConfig config) {
		this.config = config;
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

			outputSchema = getOutputSchema(inputSchema);
			stageConfigurer.setOutputSchema(outputSchema);

		}

	}

	@Override
	public Dataset<Row> compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {

		Properties props = new Properties();
		props.setProperty("input.col", config.inputCol);
		props.setProperty("output.col", config.outputCol);

		return NLP.tokenize(source, props, true);

	}
	
	@Override
	public void validateSchema() {
		
		/** INPUT COLUMN **/

		Schema.Field textCol = inputSchema.getField(config.inputCol);
		if (textCol == null) {
			throw new IllegalArgumentException(String.format(
					"[%s] The input schema must contain the field that defines the sentence annotations.", this.getClass().getName()));
		}

		isString(config.inputCol);
		
	}
	
	public Schema getOutputSchema(Schema inputSchema) {

		List<Schema.Field> fields = new ArrayList<>(inputSchema.getFields());
		
		fields.add(Schema.Field.of(config.outputCol, Schema.arrayOf(Schema.of(Schema.Type.STRING))));
		return Schema.recordOf(inputSchema.getRecordName() + ".transformed", fields);

	}	
	
	public static class SentenceTokenizerConfig extends BaseConfig {

		private static final long serialVersionUID = 1L;

		@Description("The name of the field in the input schema that contains the sentence annotations.")
		@Macro
		public String inputCol;

		@Description("The name of the field in the output schema that contains the token annotations.")
		@Macro
		public String outputCol;
		
		public void validate() {
			super.validate();

			if (Strings.isNullOrEmpty(inputCol))
				throw new IllegalArgumentException(
						String.format("[%s] The name of the field that contains the sentence annotations must not be empty.",
								this.getClass().getName()));
			
			if (Strings.isNullOrEmpty(inputCol))
				throw new IllegalArgumentException(
						String.format("[%s] The name of the field that contains the token annotations must not be empty.",
								this.getClass().getName()));
			
		}
	}
}
