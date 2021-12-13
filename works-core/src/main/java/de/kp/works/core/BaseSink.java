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

import de.kp.works.core.configuration.S3Access;
import io.cdap.cdap.api.spark.sql.DataFrames;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import io.cdap.cdap.etl.api.batch.SparkSink;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public abstract class BaseSink extends SparkSink<StructuredRecord> {

	private static final long serialVersionUID = -4938491756852655492L;

	/*
	 * Reference to input & output schema
	 */
	protected Schema inputSchema;
	protected Schema outputSchema;

	protected String BEST_MODEL = "best";
	protected String LATEST_MODEL = "latest";

	public void validateSchema(Schema inputSchema) {
	}

	@Override
	public void run(SparkExecutionPluginContext context, JavaRDD<StructuredRecord> input) throws Exception {
		/*
		 * In case of an empty input immediately return
		 * without any further processing
		 */
		if (input.isEmpty())
			return;

		if (inputSchema == null) {

			inputSchema = input.first().getSchema();
			validateSchema(inputSchema);
		}
		/*
		 * Build an Apache Spark session from the provided
		 * Spark context
		 */
		JavaSparkContext jsc = context.getSparkContext();
		/*
		 * MODEL ARTIFACT (WRITE) SUPPORT
		 *
		 * This feature either leverages the local file system
		 * or an AWS S3 bucket to store and manage trained
		 * Apache Spark ML models.
		 */
		S3Access s3Creds = new S3Access();
		if (s3Creds.nonEmpty()) {

			jsc.hadoopConfiguration().set("fs.s3a.endpoint", s3Creds.getEndpoint());

			jsc.hadoopConfiguration().set("fs.s3a.access.key", s3Creds.getAccessKey());
			jsc.hadoopConfiguration().set("fs.s3a.secret.key", s3Creds.getSecretKey());
		}

		SparkSession session = new SparkSession(jsc.sc());
		/*
		 * Transform JavaRDD<StructuredRecord> into Dataset<Row>
		 */
		StructType structType = DataFrames.toDataType(inputSchema);
		Dataset<Row> rows = SessionHelper.toDataset(input, structType, session);
		/*
		 * Compute data model from 'rows' leveraging the underlying
		 * Scala library of Predictive Works
		 */
		compute(context, rows);

	}

	public void compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {
		throw new Exception("[ERROR] Not implemented");
	}

	public void compute(SparkSession session, Dataset<Row> source) throws Exception {
		throw new Exception("[ERROR] Not implemented");
	}

	public static Schema getNonNullIfNullable(Schema schema) {
		return schema.isNullable() ? schema.getNonNullable() : schema;
	}

}
