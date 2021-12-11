package de.kp.works.ml.feature;
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
import de.kp.works.core.feature.FeatureConfig;
import de.kp.works.core.SchemaUtil;
import de.kp.works.core.feature.FeatureCompute;

@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("Tokenizer")
@Description("A transformation stage that leverages the Apache Spark ML Regex Tokenizer to split an input text into a sequence of tokens.")
public class Tokenizer extends FeatureCompute {
	/*
	 * A regex based tokenizer that extracts tokens either by using the provided regex pattern
	 * to split the text (default) or repeatedly matching the regex (if `gaps` is false).
	 * 
	 * Optional parameters also allow filtering tokens using a minimal length. It returns an 
	 * array of strings that can be empty.
	 */
	private static final long serialVersionUID = 5149156933259052782L;

	private final TokenizerConfig config;
	
	public Tokenizer(TokenizerConfig config) {
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
			
			validateSchema(inputSchema);
			/*
			 * In cases where the input schema is explicitly provided, we determine the
			 * output schema by explicitly adding the output column
			 */
			outputSchema = getOutputSchema(inputSchema, config.outputCol);
			stageConfigurer.setOutputSchema(outputSchema);

		}

	}
	
	@Override
	public void validateSchema(Schema inputSchema) {
		config.validateSchema(inputSchema);
	}

	/**
	 * A helper method to compute the output schema in that use cases where an input
	 * schema is explicitly given
	 */
	public Schema getOutputSchema(Schema inputSchema, String outputField) {

		assert inputSchema.getFields() != null;
		List<Schema.Field> fields = new ArrayList<>(inputSchema.getFields());
		
		fields.add(Schema.Field.of(outputField, Schema.arrayOf(Schema.of(Schema.Type.STRING))));
		return Schema.recordOf(inputSchema.getRecordName() + ".transformed", fields);

	}	
	
	@Override
	public Dataset<Row> compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {
		/*
		 * Transformation from [String] to Array[String]
		 */
		org.apache.spark.ml.feature.RegexTokenizer transformer = new org.apache.spark.ml.feature.RegexTokenizer();
		
		transformer.setInputCol(config.inputCol);
		transformer.setOutputCol(config.outputCol);

		transformer.setPattern(config.pattern);
		transformer.setMinTokenLength(config.minTokenLength);
		
		boolean gaps = config.gaps.equals("true");
		transformer.setGaps(gaps);

		return transformer.transform(source);

	}

	public static class TokenizerConfig extends FeatureConfig {

		private static final long serialVersionUID = -1100822249911973196L;
		
		@Description("The regex pattern used to split the input text. The pattern is used to match delimiters, if 'gaps' = true "
				+ "or tokens if 'gaps' = false: Default is '\\s+'.")		
		@Macro
		public String pattern;
		
		@Description("Minimum token length, greater than or equal to 0,  to avoid returning empty strings. Default is 1.")
		@Macro
		public Integer minTokenLength;
		
		@Description("Indicator to determine whether regex splits on gaps (true) or matches tokens (false). Default is 'true'.")
		@Macro
		public String gaps;
		
		public TokenizerConfig() {
			pattern = "\\s+";
			minTokenLength = 1;
		}
		
		public void validate() {
			super.validate();
			if (Strings.isNullOrEmpty(pattern)) {
				throw new IllegalArgumentException(
						String.format("[%s] The regex pattern must not be empty.",
								this.getClass().getName()));
			}
			
			if (minTokenLength < 0) {
				throw new IllegalArgumentException(String
						.format("[%s] The minimum token length must be nonnegative.", this.getClass().getName()));
			}
			
		}
		public void validateSchema(Schema inputSchema) {
			super.validateSchema(inputSchema);
			
			/* INPUT COLUMN */
			SchemaUtil.isString(inputSchema, inputCol);
			
		}
		
	}
}
