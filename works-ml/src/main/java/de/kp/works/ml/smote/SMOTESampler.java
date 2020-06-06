package de.kp.works.ml.smote;
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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.SparkCompute;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;

import de.kp.works.core.BaseConfig;
import de.kp.works.core.SchemaUtil;
import de.kp.works.core.smote.SMOTECompute;

@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("SMOTESampler")
@Description("A preparation stage for either building Apache Spark based classification or regression models. This stage leverages the "
		+ "SMOTE algorithm to extends a training dataset containing features & labels with synthetic data records.")
public class SMOTESampler extends SMOTECompute {
	
	private static final long serialVersionUID = 6941306314689386349L;

	private SMOTESamplerConfig config;
	
	public SMOTESampler(SMOTESamplerConfig config) {
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
			outputSchema = getOutputSchema(inputSchema);
			stageConfigurer.setOutputSchema(outputSchema);

		}

	}
	
	@Override
	public Dataset<Row> compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {
		
		SmoteBalancer balancer = new SmoteBalancer();
		balancer.setFeaturesCol(config.featuresCol);
		balancer.setLabelCol(config.labelCol);
		
		balancer.setNumHashTables(config.numHashTables);
		balancer.setBucketLength(config.bucketLength);
		
		balancer.setNumNearestNeighbors(config.numNearestNeighbors);

		return balancer.transform(source);
		
	}
	
	public void validateSchema(Schema inputSchema) {
		config.validateSchema(inputSchema);
	}
	/**
	 * A helper method to compute the output schema in that use cases where an input
	 * schema is explicitly given
	 */
	public Schema getOutputSchema(Schema inputSchema) {

		List<Schema.Field> fields = new ArrayList<>();
		
		fields.add(Schema.Field.of(config.featuresCol, Schema.arrayOf(Schema.of(Schema.Type.DOUBLE))));
		fields.add(Schema.Field.of(config.labelCol, Schema.of(Schema.Type.DOUBLE)));

		return Schema.recordOf(inputSchema.getRecordName() + ".transformed", fields);

	}	

	public static class SMOTESamplerConfig extends BaseConfig {

		private static final long serialVersionUID = -458817606829962841L;

		@Description("The name of the field in the input schema that contains the feature vector.")
		@Macro
		public String featuresCol;

		@Description("The name of the field in the input schema that contains the label.")
		@Macro
		public String labelCol;
		
		@Description("The number of hash tables used in LSH OR-amplification. LSH OR-amplification can be used to reduce the false negative rate. "
				+ "Higher values for this parameter lead to a reduced false negative rate, at the expense of added computational complexity. Default is 1.")
		@Macro
		public Integer numHashTables;

		@Description("The length of each hash bucket, a larger bucket lowers the false negative rate. The number of buckets will be "
				+ "'(max L2 norm of input vectors) / bucketLength'.")
		@Macro
		public Double bucketLength;

		@Description("The number of nearest neighbors that are taken into account by the SMOTE algorithm to interpolate synthetic feature values. "
				+ "Default is 4.")
		@Macro
		public Integer numNearestNeighbors;
		
		public SMOTESamplerConfig() {
			
			bucketLength = 2.0;
			numHashTables = 1;
			
			numNearestNeighbors = 4;
			
		}

		public void validate() {
			super.validate();

			if (Strings.isNullOrEmpty(featuresCol)) {
				throw new IllegalArgumentException(
						String.format("[%s] The name of the field that contains the feature vector must not be empty.",
								this.getClass().getName()));
			}
			if (Strings.isNullOrEmpty(labelCol)) {
				throw new IllegalArgumentException(
						String.format("[%s] The name of the field that contains the label value must not be empty.",
								this.getClass().getName()));
			}
			if (numHashTables < 1)
				throw new IllegalArgumentException(String.format(
						"[%s] The number of hash tables must be at least 1.", this.getClass().getName()));

			if (bucketLength <= 0D)
				throw new IllegalArgumentException(String.format(
						"[%s] The bucket length  must be greater than 0.0.", this.getClass().getName()));

			if (numNearestNeighbors < 1)
				throw new IllegalArgumentException(String.format(
						"[%s] The number of nearest neighbors must be at least 1.", this.getClass().getName()));
			
		}
		public void validateSchema(Schema inputSchema) {
			
			/** FEATURES COLUMN **/

			Schema.Field featuresField = inputSchema.getField(featuresCol);
			if (featuresField == null) {
				throw new IllegalArgumentException(String.format(
						"[%s] The input schema must contain the field that defines the features.", this.getClass().getName()));
			}

			SchemaUtil.isArrayOfNumeric(inputSchema, featuresCol);
			
			/** LABEL COLUMN **/

			Schema.Field labelField = inputSchema.getField(labelCol);
			if (labelField == null) {
				throw new IllegalArgumentException(String.format(
						"[%s] The input schema must contain the field that defines the label.", this.getClass().getName()));
			}
			
			SchemaUtil.isNumeric(inputSchema, labelCol);
			
		}
		
	}
}
