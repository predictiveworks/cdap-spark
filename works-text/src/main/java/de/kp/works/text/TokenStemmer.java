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
import de.kp.works.core.BaseConfig;
import de.kp.works.text.util.Names;

@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("TokenStemmer")
@Description("A transformation stage that leverages the Spark NLP Stemmer to map an input "
		+ "text field onto its normalized terms and reduce each terms to its linguistic stem. "
		+ "This stage adds an extra field to the input schema that contains the whitespace "
		+ "separated set of stems.")		
public class TokenStemmer extends TextCompute {

	private static final long serialVersionUID = -9175482610644361111L;
	private final TokenStemmerConfig config;
	
	public TokenStemmer(TokenStemmerConfig config) {
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
		return NLP.stemToken(source, config.textCol, config.stemCol);
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
		fields.add(Schema.Field.of(config.stemCol, Schema.of(Schema.Type.STRING)));
		
		return Schema.recordOf(inputSchema.getRecordName() + ".transformed", fields);

	}	
	
	public static class TokenStemmerConfig extends BaseConfig {

		private static final long serialVersionUID = 6563905240340462071L;

		@Description(Names.TEXT_COL)
		@Macro
		public String textCol;

		@Description(Names.STEM_COL)
		@Macro
		public String stemCol;
		
		public void validate() {
			super.validate();

			if (Strings.isNullOrEmpty(textCol))
				throw new IllegalArgumentException(
						String.format("[%s] The name of the field that contains the text document must not be empty.",
								this.getClass().getName()));
						
			if (Strings.isNullOrEmpty(stemCol))
				throw new IllegalArgumentException(
						String.format("[%s] The name of the field that contains the stemmed tokens must not be empty.",
								this.getClass().getName()));
			
		}
	}

}
