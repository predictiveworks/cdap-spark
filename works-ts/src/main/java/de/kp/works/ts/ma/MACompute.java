package de.kp.works.ts.ma;
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
import de.kp.works.core.time.TimeCompute;

public class MACompute extends TimeCompute {

	private static final long serialVersionUID = -874132801488333236L;

	protected static final String STATUS_FIELD = "status";

	protected Schema getOutputSchema(String timeField, String valueField, String statusField) {

		List<Schema.Field> fields = new ArrayList<>();
		
		fields.add(Schema.Field.of(timeField, Schema.of(Schema.Type.LONG)));
		fields.add(Schema.Field.of(valueField, Schema.of(Schema.Type.DOUBLE)));
		
		fields.add(Schema.Field.of(statusField, Schema.of(Schema.Type.STRING)));
		return Schema.recordOf("timeseries.forecast", fields);

	}

}
