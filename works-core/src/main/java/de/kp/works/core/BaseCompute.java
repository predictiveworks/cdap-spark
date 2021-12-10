package de.kp.works.core;
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

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.spark.sql.DataFrames;
import io.cdap.cdap.etl.api.batch.SparkCompute;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;

public abstract class BaseCompute extends SparkCompute<StructuredRecord, StructuredRecord> {

	private static final long serialVersionUID = 6855738584152026479L;

	/*
	 * Reference to input & output schema
	 */
	protected Schema inputSchema;
	protected Schema outputSchema;
	/*
	 * The model options supported to either determine the
	 * 'best' or the 'latest' model: 
	 * 
	 * The best model is computed as that model with the 
	 * smallest sum deviation computed from its deviations
	 * from the best available metric values.  
	 */
	protected static final String BEST_MODEL = "best";
	protected static final String LATEST_MODEL = "latest";
	/*
	 * The name of the column that is used to annotate the
	 * model profile to each prediction result 
	 */
	protected static final String ANNOTATION_COL = "annotation";
	
	public BaseCompute() {
	}

	/**
	 * This method is given a Spark RDD (Resilient Distributed Dataset) containing
	 * every object that is received from the previous stage. It performs Spark
	 * operations on the input to transform it into an output RDD that will be sent
	 * to the next stage.
	 */
	@Override
	public JavaRDD<StructuredRecord> transform(SparkExecutionPluginContext context, JavaRDD<StructuredRecord> input)
			throws Exception {

		/*
		 * In case of an empty input the input is immediately
		 * returned without any further processing
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
			validateSchema(inputSchema);
		}

		JavaSparkContext jsc = context.getSparkContext();
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
		return SessionHelper.fromDataset(output, outputSchema);

	}

	public Dataset<Row> compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {
		throw new Exception("[BaseCompute] Not implemented");
	}

	public Dataset<Row> compute(SparkSession session, Dataset<Row> source) throws Exception {
		throw new Exception("[BaseCompute] Not implemented");
	}

 	public void validateSchema(Schema inputSchema) {
		
	}
	
	protected Boolean isNumericType(Schema.Type dataType) {
		return SchemaUtil.isNumericType(dataType);
	}

	protected void isArrayOfDouble(String fieldName) {
		SchemaUtil.isArrayOfDouble(inputSchema, fieldName);
	}

	protected void isArrayOfNumeric(String fieldName) {
		SchemaUtil.isArrayOfNumeric(inputSchema, fieldName);
	}

	protected void isArrayOfString(String fieldName) {
		SchemaUtil.isArrayOfString(inputSchema, fieldName);
	}

	protected void isNumeric(String fieldName) {
		SchemaUtil.isNumeric(inputSchema, fieldName);
	}

	protected void isString(String fieldName) {
		SchemaUtil.isString(inputSchema, fieldName);
	}

	protected static Schema getNonNullIfNullable(Schema schema) {
		return schema.isNullable() ? schema.getNonNullable() : schema;
	}

}
