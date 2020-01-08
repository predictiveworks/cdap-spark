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

	public void compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {
		throw new Exception("[ERROR] Not implemented");
	}

}
