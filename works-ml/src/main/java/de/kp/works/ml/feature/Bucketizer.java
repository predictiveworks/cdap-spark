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
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

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
import de.kp.works.core.recording.MLUtils;

@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("Bucketizer")
@Description("A transformation stage that leverages the Apache Spark ML Feature Bucketizer to map continuous features onto feature buckets.")
public class Bucketizer extends FeatureCompute {
	/*
	 * Bucketizer transforms a column of continuous features to a column of feature buckets, where the buckets 
	 * are specified by users. It takes a parameter: splits.
	 * 
	 * With n+1 splits, there are n buckets. A bucket defined by splits x, y holds values in the range [x,y) 
	 * except the last bucket, which also includes y. Splits should be strictly increasing. 
	 * 
	 * Values at -inf, inf must be explicitly provided to cover all Double values; Otherwise, values outside 
	 * the splits specified will be treated as errors. 
	 * 
	 * Two examples of splits are Array(Double.NegativeInfinity, 0.0, 1.0, Double.PositiveInfinity) and Array(0.0, 1.0, 2.0).
	 * Note that if you have no idea of the upper and lower bounds of the targeted column, you should add Double.NegativeInfinity 
	 * and Double.PositiveInfinity as the bounds of your splits to prevent a potential out of Bucketizer bounds exception.
	 * 
	 * Note also that the splits that you provided have to be in strictly increasing order, i.e. s0 < s1 < s2 < ... < sn.
	 */

	private static final long serialVersionUID = 139261697861873381L;

	private BucketizerConfig config;
	
	public Bucketizer(BucketizerConfig config) {
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
		 * Tranformation from Numeric to Double
		 */
		BucketizerConfig bucketConfig = (BucketizerConfig)config;
		Dataset<Row> castset = MLUtils.castToDouble(source, bucketConfig.inputCol, "_input");
		
		org.apache.spark.ml.feature.Bucketizer transformer = new org.apache.spark.ml.feature.Bucketizer();
		transformer.setInputCol("_input");
		transformer.setOutputCol(bucketConfig.outputCol);

		transformer.setSplits(bucketConfig.getSplits());

		Dataset<Row> output = transformer.transform(castset).drop("_input");		
		return output;

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

	public static class BucketizerConfig extends FeatureConfig {
	
		private static final long serialVersionUID = -4048306352805978020L;
		
		@Description("A comma separated list of split points (Double values) for mapping continuous features into buckets. "
				+ "With n+1 splits, there are n buckets. A bucket defined by splits x,y holds values in the range [x,y) "
				+ "except the last bucket, which also includes y. The splits should be of length >= 3 and strictly increasing. "
				+ "Values at -infinity, infinity must be explicitly provided to cover all Double values; otherwise, values outside "
				+ "the splits specified will be treated as errors.")
		@Macro
		public String splits;

		public BucketizerConfig() {
			
		}
		
		public double[] getSplits() {
			
			String[] tokens = splits.split(",");
			List<Double> splits = new ArrayList<>();

			for (String token: tokens) {
				
				if (token.trim().toLowerCase().equals("-infinity"))
					splits.add(Double.NEGATIVE_INFINITY);
				
				if (token.trim().toLowerCase().equals("infinity"))
					splits.add(Double.POSITIVE_INFINITY);
				
				splits.add(Double.parseDouble(token.trim()));
			}

			Collections.sort(splits);
			
			Double[] array = splits.toArray(new Double[splits.size()]);
			return Stream.of(array).mapToDouble(Double::doubleValue).toArray();

		}

		public void validate() {
			super.validate();
			
			if (Strings.isNullOrEmpty(splits)) {
				throw new IllegalArgumentException(
						String.format("[%s] The split points must not be empty.", this.getClass().getName()));
			}
			
		}

		public void validateSchema(Schema inputSchema) {
			super.validateSchema(inputSchema);
			
			/** INPUT COLUMN **/
			SchemaUtil.isNumeric(inputSchema, inputCol);
			
		}
		
	}
}
