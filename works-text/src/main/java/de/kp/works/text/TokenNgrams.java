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
import de.kp.works.text.util.Names;
import de.kp.works.core.BaseConfig;

@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("TokenNgrams")
@Description("A transformation stage that leverages the Spark NLP N-gram generator to map an input "
		+ "text field onto an output field that contains its associated N-grams.")
public class TokenNgrams extends TextCompute {

	private static final long serialVersionUID = -8026917293183629882L;
	
	private TokenNgramsConfig config;
	
	public TokenNgrams(TokenNgramsConfig config) {
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
		return NLP.generateNgrams(source, config.n, config.textCol, config.ngramCol);
	}
	
	@Override
	public void validateSchema(Schema inputSchema) {
		
		/** INPUT COLUMN **/

		Schema.Field textCol = inputSchema.getField(config.textCol);
		if (textCol == null) {
			throw new IllegalArgumentException(String.format(
					"[%s] The input schema must contain the field that defines the text document.", this.getClass().getName()));
		}

		isString(config.textCol);
		
	}
	
	public Schema getOutputSchema(Schema inputSchema) {

		List<Schema.Field> fields = new ArrayList<>(inputSchema.getFields());
		
		fields.add(Schema.Field.of(config.ngramCol, Schema.arrayOf(Schema.of(Schema.Type.STRING))));
		return Schema.recordOf(inputSchema.getRecordName() + ".transformed", fields);

	}	

	public static class TokenNgramsConfig extends BaseConfig {

		private static final long serialVersionUID = -4410021712903510108L;

		@Description(Names.TEXT_COL)
		@Macro
		public String textCol;

		@Description("The name of the field in the output schema that contains the N-grams.")
		@Macro
		public String ngramCol;

		@Description("Minimum n-gram length, greater than or equal to 1. Default is 2.")
		@Macro
		public Integer n;

		public TokenNgramsConfig() {
			n = 2;
		}

		public void validate() {
			super.validate();

			if (Strings.isNullOrEmpty(textCol))
				throw new IllegalArgumentException(
						String.format("[%s] The name of the field that contains the text document must not be empty.",
								this.getClass().getName()));

			if (Strings.isNullOrEmpty(ngramCol))
				throw new IllegalArgumentException(
						String.format("[%s] The name of the field that contains the N-grams must not be empty.",
								this.getClass().getName()));

			if (n < 1) {
				throw new IllegalArgumentException(String
						.format("[%s] The minimum n-gram length must be greater than 0.", this.getClass().getName()));
			}

		}
	}

}
