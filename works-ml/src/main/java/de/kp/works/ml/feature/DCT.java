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
import de.kp.works.core.ml.MLUtils;

@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("DCT")
@Description("A transformation stage that leverages the Apache Spark ML Discrete Cosine Tranform to map a feature vector in the time domain "
		+ "into a feature vector in the frequency domain.")
public class DCT extends FeatureCompute {
	/*
	 * The Discrete Cosine Transform transforms a length N real-valued sequence in
	 * the time domain into another length N real-valued sequence in the frequency
	 * domain.
	 * 
	 * A DCT class provides this functionality, implementing the DCT-II and scaling
	 * the result by 1/2‾√ such that the representing matrix for the transform is
	 * unitary.
	 * 
	 * No shift is applied to the transformed sequence (e.g. the 0th element of the
	 * transformed sequence is the 0th DCT coefficient and not the N/2th).
	 */
	private static final long serialVersionUID = -8951658736866494139L;

	private DCTConfig config;
	
	public DCT(DCTConfig config) {
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

		List<Schema.Field> fields = new ArrayList<>(inputSchema.getFields());
		
		fields.add(Schema.Field.of(outputField, Schema.of(Schema.Type.DOUBLE)));
		return Schema.recordOf(inputSchema.getRecordName() + ".transformed", fields);

	}	
	
	@Override
	public Dataset<Row> compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {

		DCTConfig transformConfig = (DCTConfig)config;
		/*
		 * Transformation from Array[Numeric] to Array[Double]
		 * 
		 * Build internal column from input column and cast to 
		 * double vector
		 */
		Dataset<Row> vectorset = MLUtils.vectorize(source, transformConfig.inputCol, "_input", true);
		
		org.apache.spark.ml.feature.DCT transformer = new org.apache.spark.ml.feature.DCT();
		transformer.setInputCol("_input");
		/*
		 * The internal output of the transformer is an ML specific
		 * vector representation; this must be transformed into
		 * an Array[Double] to be compliant with Google CDAP
		 */		
		transformer.setOutputCol("_vector");
		
		Boolean inverse = transformConfig.inverse.equals("true") ? true : false;
		transformer.setInverse(inverse);

		Dataset<Row> transformed = transformer.transform(vectorset);		
		Dataset<Row> output = MLUtils.devectorize(transformed, "_vector", transformConfig.outputCol).drop("_input").drop("_vector");

		return output;

	}
	
	public static class DCTConfig extends FeatureConfig {

		private static final long serialVersionUID = -2746672832026751609L;

		@Description("An indicator to determine whether to perform the inverse DCT (true) or forward DCT (false). Default is 'false'.")
		@Macro
		public String inverse;
		
		public DCTConfig() {
			inverse = "false";
		}

		public void validate() {
			super.validate();
		}
		
		public void validateSchema(Schema inputSchema) {
			super.validateSchema(inputSchema);

			/** INPUT COLUMN **/
			SchemaUtil.isArrayOfNumeric(inputSchema, inputCol);
			
		}
		
	}
}
