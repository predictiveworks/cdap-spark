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

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import co.cask.cdap.etl.api.batch.SparkSink;

public abstract class BaseSink extends SparkSink<StructuredRecord> {

	private static final long serialVersionUID = -4938491756852655492L;

	protected String className;
	/*
	 * Reference to input & output schema
	 */
	protected Schema inputSchema;
	protected Schema outputSchema;

	protected Boolean isNumericType(Schema.Type dataType) {
		switch (dataType) {
		case ARRAY:
		case BOOLEAN:
		case BYTES:
		case MAP:
		case NULL:
		case RECORD:
		case ENUM:
		case STRING:
		case UNION:
			return false;
		case DOUBLE:
		case FLOAT:
		case INT:
		case LONG:
			return true;

		default:
			return false;
		}

	}

	protected void isArrayOfDouble(String fieldName) {

		Schema.Field field = inputSchema.getField(fieldName);
		Schema.Type fieldType = field.getSchema().getType();

		if (!fieldType.equals(Schema.Type.ARRAY)) {
			throw new IllegalArgumentException(
					String.format("[%s] The field that defines the model input must be an ARRAY.", className));
		}

		Schema.Type fieldCompType = field.getSchema().getComponentSchema().getType();
		if (!fieldCompType.equals(Schema.Type.DOUBLE)) {
			throw new IllegalArgumentException(
					String.format("[%s] The data type of the input field components must be a DOUBLE.", className));
		}
		
	}

	protected void isArrayOfNumeric(String fieldName) {

		Schema.Field field = inputSchema.getField(fieldName);
		Schema.Type fieldType = field.getSchema().getType();

		if (!fieldType.equals(Schema.Type.ARRAY)) {
			throw new IllegalArgumentException(
					String.format("[%s] The field that defines the model input must be an ARRAY.", this.getClass().getName()));
		}

		Schema.Type fieldCompType = field.getSchema().getComponentSchema().getType();
		if (!isNumericType(fieldCompType)) {
			throw new IllegalArgumentException(
					String.format("[%s] The data type of the input field components must be NUMERIC.", this.getClass().getName()));
		}
		
	}

	protected void isArrayOfString(String fieldName) {

		Schema.Field field = inputSchema.getField(fieldName);
		Schema.Type fieldType = field.getSchema().getType();

		if (!fieldType.equals(Schema.Type.ARRAY)) {
			throw new IllegalArgumentException(
					String.format("[%s] The field that defines the model input must be an ARRAY.", className));
		}

		Schema.Type fieldCompType = field.getSchema().getComponentSchema().getType();
		if (!fieldCompType.equals(Schema.Type.STRING)) {
			throw new IllegalArgumentException(
					String.format("[%s] The data type of the input field components must be a STRING.", className));
		}
		
	}

	protected void isString(String fieldName) {

		Schema.Field field = inputSchema.getField(fieldName);
		Schema.Type fieldType = field.getSchema().getType();

		if (!fieldType.equals(Schema.Type.STRING)) {
			throw new IllegalArgumentException(
					String.format("[%s] The field that defines the input must be a STRING.", this.getClass().getName()));
		}

	}

	public void compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {
		throw new Exception("[ERROR] Not implemented");
	}

}
