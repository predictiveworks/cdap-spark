package de.kp.works.text.matching;
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
import de.kp.works.text.NLP;

@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("RegexMatcher")
@Description("A transformation stage that leverages the Spark NLP Regex Matcher to detected provided ."
		+ "Regex rules in the input text document.")
public class RegexMatcher extends TextCompute {

	private static final long serialVersionUID = -5699840126647930210L;

	private RegexMatcherConfig config;
	
	public RegexMatcher(RegexMatcherConfig config) {
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
		props.setProperty("input.col", config.textCol);
		props.setProperty("output.col", config.outputCol);
		
		props.setProperty("rules", config.getRules());		
		props.setProperty("delimiter", config.delimiter);

		return NLP.matchRegex(source, props);

	}
	
	@Override
	public void validateSchema(Schema inputSchema) {
		
		/** INPUT COLUMN **/

		Schema.Field textCol = inputSchema.getField(config.textCol);
		if (textCol == null) {
			throw new IllegalArgumentException(String.format(
					"[%s] The input schema must contain the field that defines the text.", this.getClass().getName()));
		}

		isString(config.textCol);
		
	}
	
	public Schema getOutputSchema(Schema inputSchema) {

		List<Schema.Field> fields = new ArrayList<>(inputSchema.getFields());
		
		fields.add(Schema.Field.of(config.outputCol, Schema.arrayOf(Schema.of(Schema.Type.STRING))));
		return Schema.recordOf(inputSchema.getRecordName() + ".transformed", fields);

	}	
	
	public static class RegexMatcherConfig extends BaseMatcherConfig {

		private static final long serialVersionUID = 1461469959716413112L;

		@Description("A delimiter separated list of Regex rules.")
		@Macro
		private String rules;

		@Description("The delimiter used to separate the different Regex rules.")
		@Macro
		private String delimiter;
		
		public RegexMatcherConfig() {
			delimiter = ",";
		}

		public String getRules() {
			
			String cleaned = rules.replaceAll("\\r\\n|\\r|\\n", " ");
			return cleaned;
		}
		
		public void validate() {
			super.validate();

			if (Strings.isNullOrEmpty(rules)) {
				throw new IllegalArgumentException(
						String.format("[%s] The Regex rules must not be empty.", this.getClass().getName()));
			}

			if (Strings.isNullOrEmpty(delimiter)) {
				throw new IllegalArgumentException(
						String.format("[%s] The rule delimiter must not be empty.", this.getClass().getName()));
			}

		}
		
	}
}
