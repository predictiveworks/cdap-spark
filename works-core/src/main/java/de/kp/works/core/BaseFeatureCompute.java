package de.kp.works.core;
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

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.spark.sql.DataFrames;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;

public class BaseFeatureCompute extends BaseCompute {

	private static final long serialVersionUID = -852876404206487204L;
	
	protected FileSet modelFs;
	protected Table modelMeta;

	protected BaseFeatureConfig config;
	
	@Override
	public JavaRDD<StructuredRecord> transform(SparkExecutionPluginContext context, JavaRDD<StructuredRecord> input)
			throws Exception {

		JavaSparkContext jsc = context.getSparkContext();
		/*
		 * In case of an empty input the input is immediately returned without any
		 * furthr processing
		 */
		if (input.isEmpty()) {
			return input;
		}
		/*
		 * Determine input schema: first, check whether the input schema is already
		 * provided by a previous initializing or preparing step
		 */
		if (inputSchema == null) {

			inputSchema = input.first().getSchema();
			validateSchema(inputSchema, config);

		}

		SparkSession session = new SparkSession(jsc.sc());

		/*
		 * STEP #1: Transform JavaRDD<StructuredRecord> into Dataset<Row>
		 */
		StructType structType = DataFrames.toDataType(inputSchema);
		Dataset<Row> rows = SessionHelper.toDataset(input, structType, session);

		/*
		 * STEP #2: Compute source with underlying Scala library and derive the output
		 * schema dynamically from the computed dataset
		 */
		Dataset<Row> output = compute(context, rows);
		if (outputSchema == null) {
			outputSchema = DataFrames.toSchema(output.schema());
		}
		/*
		 * STEP #3: Transform Dataset<Row> into JavaRDD<StructuredRecord>
		 */
		JavaRDD<StructuredRecord> records = SessionHelper.fromDataset(output, outputSchema);
		return records;

	}

	protected void validateSchema(Schema inputSchema, BaseFeatureConfig config) {

		/** INPUT COLUMN **/

		Schema.Field inputCol = inputSchema.getField(config.inputCol);
		if (inputCol == null) {
			throw new IllegalArgumentException(String.format(
					"[%s] The input schema must contain the field that defines the features.", this.getClass().getName()));
		}
	}

	protected void isArrayOfDouble(String fieldName) {

		Schema.Field field = inputSchema.getField(fieldName);
		Schema.Type fieldType = field.getSchema().getType();

		if (!fieldType.equals(Schema.Type.ARRAY)) {
			throw new IllegalArgumentException(
					String.format("[%s] The field that defines the model input must be an ARRAY.", this.getClass().getName()));
		}

		Schema.Type fieldCompType = field.getSchema().getComponentSchema().getType();
		if (!fieldCompType.equals(Schema.Type.DOUBLE)) {
			throw new IllegalArgumentException(
					String.format("[%s] The data type of the input field components must be a DOUBLE.", this.getClass().getName()));
		}
		
	}

	protected void isArrayOfString(String fieldName) {

		Schema.Field field = inputSchema.getField(fieldName);
		Schema.Type fieldType = field.getSchema().getType();

		if (!fieldType.equals(Schema.Type.ARRAY)) {
			throw new IllegalArgumentException(
					String.format("[%s] The field that defines the model input must be an ARRAY.", this.getClass().getName()));
		}

		Schema.Type fieldCompType = field.getSchema().getComponentSchema().getType();
		if (!fieldCompType.equals(Schema.Type.STRING)) {
			throw new IllegalArgumentException(
					String.format("[%s] The data type of the input field components must be a STRING.", this.getClass().getName()));
		}
		
	}
	
}
