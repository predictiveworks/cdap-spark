package de.kp.works.ts.ar;
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

import io.cdap.cdap.api.data.schema.Schema;
import de.kp.works.core.time.TimeCompute;
import de.kp.works.ts.ForecastAssembler;

public class ARCompute extends TimeCompute {

	private static final long serialVersionUID = -1934576305579477480L;

	/*
	 * An auto regression model directly operates on the time and value field of the
	 * incoming schema; we expect that time engineering is restricted to
	 * aggregation, interpolation or sampling.
	 * 
	 * This does not generate any artificial features for model building, and
	 * therefore, no previous annotation metadata exist.
	 */
	protected Schema getOutputSchema(String timeField, String valueField, String statusField) {

		List<Schema.Field> fields = new ArrayList<>();

		fields.add(Schema.Field.of(timeField, Schema.of(Schema.Type.LONG)));
		fields.add(Schema.Field.of(valueField, Schema.of(Schema.Type.DOUBLE)));

		fields.add(Schema.Field.of(statusField, Schema.of(Schema.Type.STRING)));
		fields.add(Schema.Field.of(ANNOTATION_COL, Schema.of(Schema.Type.STRING)));

		return Schema.recordOf("timeseries.forecast", fields);

	}

	protected Dataset<Row> assembleAndAnnotate(Dataset<Row> observations, Dataset<Row> forecast, String timeCol,
			String valueCol) {

		ForecastAssembler assembler = new ForecastAssembler(timeCol, valueCol, STATUS_FIELD);
		Dataset<Row> assembled = assembler.assemble(observations, forecast);

		return annotate(assembled, TIME_TYPE);

	}

}
