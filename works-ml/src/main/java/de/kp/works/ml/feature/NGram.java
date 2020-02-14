package de.kp.works.ml.feature;
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

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import de.kp.works.core.feature.FeatureConfig;
import de.kp.works.core.SchemaUtil;
import de.kp.works.core.feature.FeatureCompute;

@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("NGram")
@Description("A transformation stage that leverages Apache Spark ML N-Gram transformer to convert the input array of string into "
		+ "an array of n-grams.")
public class NGram extends FeatureCompute {
	/*
	 * A feature transformer that converts the input array of strings into an array
	 * of n-grams. Null values in the input array are ignored.
	 * 
	 * It returns an array of n-grams where each n-gram is represented by a
	 * space-separated string of words.
	 *
	 * When the input is empty, an empty array is returned. When the input array
	 * length is less than n (number of elements per n-gram), no n-grams are
	 * returned.
	 */
	private static final long serialVersionUID = -3545405220776080200L;

	private NGramConfig config;
	
	public NGram(NGramConfig config) {
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
	public Dataset<Row> compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {
		
		/*
		 * Transformation from Array[String] to Array[String]
		 */
		org.apache.spark.ml.feature.NGram transformer = new org.apache.spark.ml.feature.NGram();
		transformer.setInputCol(config.inputCol);

		transformer.setOutputCol(config.outputCol);
		transformer.setN(config.n);

		Dataset<Row> output = transformer.transform(source);		
		return output;

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

		List<Schema.Field> fields = new ArrayList<>(inputSchema.getFields());
		
		fields.add(Schema.Field.of(outputField, Schema.arrayOf(Schema.of(Schema.Type.STRING))));
		return Schema.recordOf(inputSchema.getRecordName() + ".transformed", fields);

	}	

	public static class NGramConfig extends FeatureConfig {

		private static final long serialVersionUID = 3771824648631925491L;

		@Description("Minimum n-gram length, greater than or equal to 1. Default is 2.")
		@Macro
		public Integer n;

		public NGramConfig() {
			n = 2;
		}

		public void validate() {
			super.validate();

			if (n < 1) {
				throw new IllegalArgumentException(String
						.format("[%s] The minimum n-gram length must be greater than 0.", this.getClass().getName()));
			}

		}
		
		public void validateSchema(Schema inputSchema) {
			super.validateSchema(inputSchema);
			
			/** INPUT COLUMN **/
			SchemaUtil.isArrayOfString(inputSchema, inputCol);
			
		}
		
	}
}
