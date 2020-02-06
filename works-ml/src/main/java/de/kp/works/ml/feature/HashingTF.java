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
import de.kp.works.ml.MLUtils;

@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("HashingTF")
@Description("A transformation stage that leverages the Apache Spark HashingTF and maps a sequence of terms to their "
		+ "term frequencies using the hashing trick. Currently the Austin Appleby's MurmurHash 3 algorithm is used.")
public class HashingTF extends FeatureCompute {
	/*
	 * HashingTF is a Transformer which takes sets of terms and converts those sets into 
	 * fixed-length feature vectors. In text processing, a 'set of terms' might be a bag 
	 * of words. HashingTF utilizes the hashing trick. 
	 * 
	 * A raw feature is mapped into an index (term) by applying a hash function. The hash 
	 * function used here is MurmurHash 3. Then term frequencies are calculated based on 
	 * the mapped indices. 
	 * 
	 * This approach avoids the need to compute a global term-to-index map, which can be 
	 * expensive for a large corpus, but it suffers from potential hash collisions, where 
	 * different raw features may become the same term after hashing. 
	 * 
	 * To reduce the chance of collision, we can increase the target feature dimension, i.e. 
	 * the number of buckets of the hash table. Since a simple modulo is used to transform 
	 * the hash function to a column index, it is advisable to use a power of two as the 
	 * feature dimension, otherwise the features will not be mapped evenly to the columns. 
	 * 
	 * The default feature dimension is 218=262,144.
	 */
	private static final long serialVersionUID = 8394737976482170698L;

	private HashTFConfig config;
	
	public HashingTF(HashTFConfig config) {
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
	
	@Override
	public Dataset<Row> compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {
		/*
		 * Transformation from Array[String] to Array[Double]
		 */
		org.apache.spark.ml.feature.HashingTF transformer = new org.apache.spark.ml.feature.HashingTF();
		transformer.setInputCol(config.inputCol);
		/*
		 * The internal output of the hasher is an ML specific
		 * vector representation; this must be transformed into
		 * an Array[Double] to be compliant with Google CDAP
		 */		
		transformer.setOutputCol("_vector");
		transformer.setNumFeatures(config.numFeatures);

		Dataset<Row> transformed = transformer.transform(source);
		Dataset<Row> output = MLUtils.devectorize(transformed, "_vector", config.outputCol).drop("_vector");

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
	
	public static class HashTFConfig extends FeatureConfig {

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
		
		public void validateSchema(Schema inputSchema) {
			super.validateSchema(inputSchema);
			
			/** INPUT COLUMN **/
			SchemaUtil.isArrayOfString(inputSchema, inputCol);
			
		}
		
	}

}
