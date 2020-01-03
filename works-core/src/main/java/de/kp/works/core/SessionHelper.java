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
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.spark.sql.DataFrames;

public class SessionHelper {

	/**
	 * A helper method to transform a CDAP specific JavaRDD<StructuredRecord> into
	 * an Apache Spark Dataset<Row>
	 * 
	 * @param input
	 * @param structType
	 * @param spark
	 * @return
	 */
	public static Dataset<Row> toDataset(JavaRDD<StructuredRecord> input, StructType structType, SparkSession spark) {

		JavaRDD<Row> rows = input.map(new RecordToRow(structType));
		Dataset<Row> dataset = spark.createDataFrame(rows, structType);

		return dataset;

	}

	/**
	 * A helper method to transform an Apache Spark Dataset<Row> into a CDAP
	 * specific JavaRDD<StructuredRecord>
	 * 
	 * @param output
	 * @param schema
	 * @return
	 */
	public static JavaRDD<StructuredRecord> fromDataset(Dataset<Row> output, Schema schema) {

		JavaRDD<StructuredRecord> records = output.javaRDD().map(new RowToRecord(schema));
		return records;

	}

	public static final class RecordToRow implements Function<StructuredRecord, Row> {

		private static final long serialVersionUID = -1486593666144270640L;
		private final StructType structType;

		public RecordToRow(StructType structType) {
			this.structType = structType;
		}

		@Override
		public Row call(StructuredRecord rec) throws Exception {
			return DataFrames.toRow(rec, structType);
		}

	}

	public static final class RowToRecord implements Function<Row, StructuredRecord> {

		private static final long serialVersionUID = -8179090873024375879L;
		private final Schema schema;

		public RowToRecord(Schema schema) {
			this.schema = schema;
		}

		@Override
		public StructuredRecord call(Row row) throws Exception {
			return DataFrames.fromRow(row, schema);
		}
	}

}
