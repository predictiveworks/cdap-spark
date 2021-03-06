package de.kp.works.core.chart;
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

import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.dataset.table.Put;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.api.spark.sql.DataFrames;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import io.cdap.cdap.etl.api.batch.SparkPluginContext;
import de.kp.works.core.BaseSink;
import de.kp.works.core.SessionHelper;

public class ChartSink extends BaseSink {

	private static final long serialVersionUID = -3176209571691166582L;

	@Override
	public void prepareRun(SparkPluginContext context) throws Exception {
		;
	}

	@Override
	public void run(SparkExecutionPluginContext context, JavaRDD<StructuredRecord> input) throws Exception {

		JavaSparkContext jsc = context.getSparkContext();
		if (input.isEmpty())
			return;

		if (inputSchema == null) {
			
			inputSchema = input.first().getSchema();
			validateSchema(inputSchema);
		}

		SparkSession session = new SparkSession(jsc.sc());

		StructType structType = DataFrames.toDataType(inputSchema);
		Dataset<Row> rows = SessionHelper.toDataset(input, structType, session);

		compute(context, rows);

	}
	
	public void writeDataset(Table table, String timeCol, String valueCol, Dataset<Row> dataset) {
		
		StructType schema = dataset.schema();
		
		Field timeField = new Field(schema.fieldIndex(timeCol), timeCol);
		Field valueField = new Field(schema.fieldIndex(valueCol), valueCol);
		
		for (Row row : dataset.collectAsList()) {
			
			/* Key & timestamp */
			Long ts = row.getLong(timeField.index);			
			byte[] key = Bytes.toBytes(ts);
			
			Put put = new Put(key);
			put.add(timeField.name, ts);

			/* Value field */
			put.add(valueField.name, row.getDouble(valueField.index));
			
			table.put(put);
			
		}
		
	}
	
	public static class Field {
		
		public Integer index;
		public String name;
		
		public Field(Integer index, String name) {
			this.index = index;
			this.name = name;
		}
		
	}
}
