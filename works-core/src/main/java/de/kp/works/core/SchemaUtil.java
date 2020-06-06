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

import io.cdap.cdap.api.data.schema.Schema;

public class SchemaUtil {

	public static Boolean isNumericType(Schema.Type dataType) {
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

	public static Boolean isTimeType(Schema.Type dataType) {
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
		case DOUBLE:
		case FLOAT:
		case INT:
			return false;
		case LONG:
			return true;

		default:
			return false;
		}

	}

	public static Boolean isIntegerType(Schema.Type dataType) {
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
		case DOUBLE:
		case FLOAT:
		case LONG:
			return false;
		case INT:
			return true;

		default:
			return false;
		}

	}

	public static void isArrayOfDouble(Schema schema, String fieldName) {

		Schema.Field field = schema.getField(fieldName);
		Schema fieldSchema = getNonNullIfNullable(field.getSchema());

		Schema.Type fieldType = fieldSchema.getType();

		if (!fieldType.equals(Schema.Type.ARRAY)) {
			throw new IllegalArgumentException(String.format(
					"[%s] The field that defines the model input must be an ARRAY.", SchemaUtil.class.getName()));
		}

		Schema.Type fieldCompType = getNonNullIfNullable(fieldSchema.getComponentSchema()).getType();
		if (!fieldCompType.equals(Schema.Type.DOUBLE)) {
			throw new IllegalArgumentException(String.format(
					"[%s] The data type of the input field components must be a DOUBLE.", SchemaUtil.class.getName()));
		}

	}

	public static void isArrayOfNumeric(Schema schema, String fieldName) {

		Schema.Field field = schema.getField(fieldName);
		Schema fieldSchema = getNonNullIfNullable(field.getSchema());

		Schema.Type fieldType = fieldSchema.getType();

		if (!fieldType.equals(Schema.Type.ARRAY)) {
			throw new IllegalArgumentException(String.format(
					"[%s] The field that defines the model input must be an ARRAY.", SchemaUtil.class.getName()));
		}

		Schema.Type fieldCompType = getNonNullIfNullable(fieldSchema.getComponentSchema()).getType();
		if (!isNumericType(fieldCompType)) {
			throw new IllegalArgumentException(String.format(
					"[%s] The data type of the input field components must be NUMERIC.", SchemaUtil.class.getName()));
		}

	}

	public static void isArrayOfString(Schema schema, String fieldName) {

		Schema.Field field = schema.getField(fieldName);
		Schema fieldSchema = getNonNullIfNullable(field.getSchema());

		Schema.Type fieldType = fieldSchema.getType();

		if (!fieldType.equals(Schema.Type.ARRAY)) {
			throw new IllegalArgumentException(String.format(
					"[%s] The field that defines the model input must be an ARRAY.", SchemaUtil.class.getName()));
		}

		Schema.Type fieldCompType = getNonNullIfNullable(fieldSchema.getComponentSchema()).getType();
		if (!fieldCompType.equals(Schema.Type.STRING)) {
			throw new IllegalArgumentException(String.format(
					"[%s] The data type of the input field components must be a STRING.", SchemaUtil.class.getName()));
		}

	}

	public static void isNumeric(Schema schema, String fieldName) {

		Schema.Field field = schema.getField(fieldName);
		Schema.Type fieldType = getNonNullIfNullable(field.getSchema()).getType();

		if (!isNumericType(fieldType)) {
			throw new IllegalArgumentException(String.format("[%s] The field that defines the input must be NUMERIC.",
					SchemaUtil.class.getName()));
		}

	}

	public static void isString(Schema schema, String fieldName) {

		Schema.Field field = schema.getField(fieldName);
		Schema.Type fieldType = getNonNullIfNullable(field.getSchema()).getType();

		if (!fieldType.equals(Schema.Type.STRING)) {
			throw new IllegalArgumentException(String.format("[%s] The field that defines the input must be a STRING.",
					SchemaUtil.class.getName()));
		}

	}

	public static Schema getNonNullIfNullable(Schema schema) {
		return schema.isNullable() ? schema.getNonNullable() : schema;
	}

}
