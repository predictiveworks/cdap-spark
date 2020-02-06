package de.kp.works.core.time;
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
import de.kp.works.core.BaseCompute;

public class TimeCompute extends BaseCompute {

	private static final long serialVersionUID = -1433147745214918467L;
	/*
	 * Reference to input & output schema
	 */
	protected Schema inputSchema;
	protected Schema outputSchema;

	/**
	 * The value data type is changed to DOUBLE
	 */
	protected Schema getOutputSchema(Schema inputSchema, String valueCol) {
		
		List<Schema.Field> outfields = new ArrayList<>();
		for (Schema.Field field: inputSchema.getFields()) {
			
			if (field.getName().equals(valueCol)) {
				outfields.add(Schema.Field.of(valueCol, Schema.of(Schema.Type.DOUBLE)));
				
			} else
				outfields.add(field);
		}
		
		return Schema.recordOf(inputSchema.getRecordName(), outfields);

	}

}
