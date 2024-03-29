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
import de.kp.works.core.recording.MLUtils;

@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("Normalizer")
@Description("A transformation stage to normalize a feature vector to have unit norm using the given p-norm.")
public class Normalizer extends FeatureCompute {

	private static final long serialVersionUID = 8637506241398507943L;

	private final NormalizerConfig config;
	
	public Normalizer(NormalizerConfig config) {
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
		
		fields.add(Schema.Field.of(outputField, Schema.arrayOf(Schema.of(Schema.Type.DOUBLE))));
		return Schema.recordOf(inputSchema.getRecordName() + ".transformed", fields);

	}	
	
	@Override
	public Dataset<Row> compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {
		/*
		 * Transformation from Array[Numeric] to Array[Double]
		 * 
		 * Build internal column from input column and cast to 
		 * double vector
		 */
		Dataset<Row> vectorset = MLUtils.vectorize(source, config.inputCol, "_input", true);
		
		org.apache.spark.ml.feature.Normalizer transformer = new org.apache.spark.ml.feature.Normalizer();
		transformer.setInputCol("_input");
		/*
		 * The internal output of the binarizer is an ML specific
		 * vector representation; this must be transformed into
		 * an Array[Double] to be compliant with Google CDAP
		 */
		transformer.setOutputCol("_vector");
		transformer.setP((double)config.norm);

		Dataset<Row> transformed = transformer.transform(vectorset);
		return MLUtils.devectorize(transformed, "_vector", config.outputCol).drop("_input").drop("_vector");
	    		
	}
		
	public static class NormalizerConfig extends FeatureConfig {

		private static final long serialVersionUID = 4977817064737125270L;

		@Description("The p-norm to use for normalization. Supported values are '1' and '2'. Default is 2.")
		@Macro
		public Integer norm;
		
		public NormalizerConfig() {
			norm = 2;
		}
		
		public void validate() {
			super.validate();
			
			if (!(norm == 1 || norm == 2)) {
				throw new IllegalArgumentException(String
						.format("[%s] The p-norm must be either 1 or 2.", this.getClass().getName()));
			}

		}
		
		public void validateSchema(Schema inputSchema) {
			super.validateSchema(inputSchema);
			
			SchemaUtil.isArrayOfNumeric(inputSchema, inputCol);
			
		}
		
	}
}
