package de.kp.works.text;
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
import de.kp.works.core.text.TextCompute;
import de.kp.works.text.util.Names;
import de.kp.works.core.BaseConfig;

@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("TokenNormalizer")
@Description("A transformation stage that leverages the Spark NLP Normalizer to map an input "
		+ "text field onto an output field that contains normalized tokens. The Normalizer "
		+ "will clean up each token, taking as input column token out from the Tokenizer, "
		+ "and putting normalized tokens in the normal column. Cleaning up includes removing "
		+ "any non-character strings.")
public class TokenNormalizer extends TextCompute {

	private static final long serialVersionUID = 7292639821710358852L;
	private final TokenNormalizerConfig config;
	
	public TokenNormalizer(TokenNormalizerConfig config) {
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
			
			outputSchema = getOutputSchema(inputSchema);
			stageConfigurer.setOutputSchema(outputSchema);

		}

	}

	@Override
	public Dataset<Row> compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {
		return NLP.normalizeToken(source, config.textCol, config.normCol);
	}
	
	@Override
	public void validateSchema(Schema inputSchema) {
		
		/* INPUT COLUMN */

		Schema.Field textCol = inputSchema.getField(config.textCol);
		if (textCol == null) {
			throw new IllegalArgumentException(String.format(
					"[%s] The input schema must contain the field that defines the text document.", this.getClass().getName()));
		}

		isString(config.textCol);
		
	}
	
	public Schema getOutputSchema(Schema inputSchema) {

		assert inputSchema.getFields() != null;
		List<Schema.Field> fields = new ArrayList<>(inputSchema.getFields());
		
		fields.add(Schema.Field.of(config.normCol, Schema.arrayOf(Schema.of(Schema.Type.STRING))));
		return Schema.recordOf(inputSchema.getRecordName() + ".transformed", fields);

	}	
	
	public static class TokenNormalizerConfig extends BaseConfig {

		private static final long serialVersionUID = -2180450887445343238L;

		@Description(Names.TEXT_COL)
		@Macro
		public String textCol;

		@Description(Names.NORM_COL)
		@Macro
		public String normCol;
		
		public void validate() {
			super.validate();

			if (Strings.isNullOrEmpty(textCol))
				throw new IllegalArgumentException(
						String.format("[%s] The name of the field that contains the text document must not be empty.",
								this.getClass().getName()));
			
			if (Strings.isNullOrEmpty(normCol))
				throw new IllegalArgumentException(
						String.format("[%s] The name of the field that contains the normalized tokens must not be empty.",
								this.getClass().getName()));
			
		}
	}
}
