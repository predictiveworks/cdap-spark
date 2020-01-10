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

import java.util.ArrayList;
import java.util.List;

import co.cask.cdap.api.data.schema.Schema;

public class BaseTimeCompute extends BaseCompute {

	private static final long serialVersionUID = -1433147745214918467L;
	/*
	 * Reference to input & output schema
	 */
	protected Schema inputSchema;
	protected Schema outputSchema;
	
	protected BaseTimeConfig config;
	protected String className;

	protected void validateSchema(Schema inputSchema, BaseTimeConfig config) {

		/** TIME COLUMN **/

		Schema.Field timeCol = inputSchema.getField(config.timeCol);
		if (timeCol == null) {
			throw new IllegalArgumentException(String.format(
					"[%s] The input schema must contain the field that defines the timestamp.", className));
		}

		Schema.Type timeColType = timeCol.getSchema().getType();
		if (!timeColType.equals(Schema.Type.LONG)) {
			throw new IllegalArgumentException(
					String.format("[%s] The field that defines the timestamp must be a LONG.", className));
		}

		/** VALUE COLUMN **/

		Schema.Field valueCol = inputSchema.getField(config.valueCol);
		if (valueCol == null) {
			throw new IllegalArgumentException(String
					.format("[%s] The input schema must contain the field that defines the value.", className));
		}

		Schema.Type labelType = valueCol.getSchema().getType();
		/*
		 * The value field must be a numeric data type (double, float, int, long), which then
		 * is casted to Double (see classification trainer)
		 */
		if (isNumericType(labelType) == false) {
			throw new IllegalArgumentException("The data type of the value field must be numeric.");
		}
	}

	/**
	 * A helper method to compute the output schema in that use cases where an input
	 * schema is explicitly given
	 */
	protected Schema getOutputSchema(Schema inputSchema, String predictionField) {
		
		List<Schema.Field> outfields = new ArrayList<>();
		for (Schema.Field field: inputSchema.getFields()) {
			
			if (field.getName().equals(config.valueCol)) {
				outfields.add(Schema.Field.of(config.valueCol, Schema.of(Schema.Type.DOUBLE)));
				
			} else
				outfields.add(field);
		}
		
		return Schema.recordOf(inputSchema.getRecordName(), outfields);

	}

}
