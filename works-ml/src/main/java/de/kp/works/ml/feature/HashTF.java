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

import org.apache.spark.ml.feature.HashingTF;
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
import de.kp.works.core.BaseFeatureCompute;
import de.kp.works.core.BaseFeatureConfig;

@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("HashTF")
@Description("A transformation stage that leverages the Apache Spark HashingTF and maps a sequence of terms to their "
		+ "term frequencies using the hashing trick. Currently the Austin Appleby's MurmurHash 3 algorithm is used.")
public class HashTF extends BaseFeatureCompute {

	private static final long serialVersionUID = 8394737976482170698L;

	private HashTFConfig config;

	public HashTF(HashTFConfig config) {
		this.config = config;
	}
	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {

		((HashTFConfig)config).validate();

		StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
		/*
		 * Try to determine input and output schema; if these schemas are not explicitly
		 * specified, they will be inferred from the provided data records
		 */
		inputSchema = stageConfigurer.getInputSchema();
		if (inputSchema != null) {
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

		HashingTF transformer = new HashingTF();

		transformer.setInputCol(config.inputCol);
		transformer.setOutputCol(config.outputCol);

		transformer.setNumFeatures(config.numFeatures);

		Dataset<Row> output = transformer.transform(source);
		return output;

	}

	/**
	 * A helper method to compute the output schema in that use cases where an input
	 * schema is explicitly given
	 */
	public Schema getOutputSchema(Schema inputSchema, String outputField) {

		List<Schema.Field> fields = new ArrayList<>(inputSchema.getFields());
		
		fields.add(Schema.Field.of(outputField, Schema.arrayOf(Schema.of(Schema.Type.DOUBLE))));
		return Schema.recordOf(inputSchema.getRecordName() + ".transformed", fields);

	}	
	
	public static class HashTFConfig extends BaseFeatureConfig {

		private static final long serialVersionUID = 6791984345238136178L;

		@Description("The nonnegative number of features to transform a sequence of terms into.")
		@Macro
		public Integer numFeatures;
		
		public void validate() {
			super.validate();
			
			if (numFeatures == null || numFeatures <= 0) {
				throw new IllegalArgumentException(String
						.format("[%s] The number of features must be greater than 0.", this.getClass().getName()));
			}

		}
	}

}
