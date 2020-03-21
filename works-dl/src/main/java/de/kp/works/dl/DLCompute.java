package de.kp.works.dl;
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

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession.Builder;
import org.apache.spark.sql.types.StructType;

import com.intel.analytics.bigdl.utils.Engine;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.spark.sql.DataFrames;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import de.kp.works.core.BaseCompute;
import de.kp.works.core.SessionHelper;
import scala.Tuple2;

/*
 * The base [SparkCompute] for all Analytics Zoo and BigDL
 * pipelines. Major difference to other SparkComputes is 
 * the requirement to initiate the BigDL engine. 
 */
public class DLCompute extends BaseCompute {

	private static final long serialVersionUID = 4483705269819606760L;
	/**
	 * This method is given a Spark RDD (Resilient Distributed Dataset) containing
	 * every object that is received from the previous stage. It performs Spark
	 * operations on the input to transform it into an output RDD that will be sent
	 * to the next stage.
	 */
	@Override
	public JavaRDD<StructuredRecord> transform(SparkExecutionPluginContext context, JavaRDD<StructuredRecord> input)
			throws Exception {

		JavaSparkContext jsc = context.getSparkContext();
		/*
		 * In case of an empty input the input is immediately 
		 * returned without any further processing
		 */
		if (input.isEmpty()) {
			return input;
		}
		/*
		 * Determine input schema: first, check whether the input
		 * schema is already provided by a previous initializing 
		 * or preparing step
		 */
		if (inputSchema == null) {
			inputSchema = input.first().getSchema();
			validateSchema(inputSchema);
		}
		/*
		 * STEP #1: Works DL is based on Intel's Analytics Zoo. 
		 * This demands for a slightly different generation of 
		 * the respective Spark session
		 */		
		SparkSession session = getSession(jsc.sc());
		Engine.init();

		/*
		 * STEP #2: Transform JavaRDD<StructuredRecord> into 
		 * Dataset<Row>
		 */
		StructType structType = DataFrames.toDataType(inputSchema);
		Dataset<Row> rows = SessionHelper.toDataset(input, structType, session);

		/*
		 * STEP #3: Compute source with underlying Scala library 
		 * and derive the output schema dynamically from the computed 
		 * dataset
		 */
		Dataset<Row> output = compute(context, rows);
		if (outputSchema == null) {
			outputSchema = DataFrames.toSchema(output.schema());
		}
		/*
		 * STEP #4: Transform Dataset<Row> into JavaRDD<StructuredRecord>
		 */
		JavaRDD<StructuredRecord> records = SessionHelper.fromDataset(output, outputSchema);
		return records;

	}
	/*
	 * Analytics Zoo and its underlying BigDL library requires
	 * for a specific set of Spark parameters; we have to build
	 * a new SparkSession and assign these parameters to get the
	 * start of the BigDL engine right.
	 */
	private SparkSession getSession(SparkContext sc) {
		/*
		 * BigDL : see spark-bigdl.conf
		 */
		SparkConf sparkConf = sc.getConf();

		sparkConf.set("spark.shuffle.reduceLocality.enabled", "false");
		sparkConf.set("spark.shuffle.blockTransferService", "nio");
		sparkConf.set("spark.scheduler.minRegisteredResourcesRatio", "1.0");
		sparkConf.set("spark.speculation", "false");
		/*
		 * Build a new session
		 */
		Builder builder = SparkSession.builder();
		Tuple2<String, String>[] params = sparkConf.getAll();
		for (Tuple2<String, String> param : params) {

			String key = param._1();
			String value = param._2();

			if (key.equals("spark.master"))
				builder.master(value);

			else if (key.equals("spark.app.name"))
				builder.master(value);

			else
				builder.config(key, value);

		}

		return builder.getOrCreate();
		
	}

}
