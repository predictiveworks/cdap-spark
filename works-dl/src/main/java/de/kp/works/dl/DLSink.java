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

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.spark.sql.DataFrames;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import io.cdap.cdap.etl.api.batch.SparkPluginContext;
import de.kp.works.core.BaseSink;
import de.kp.works.core.SessionHelper;
import scala.Tuple2;
/*
 * The base [SparkSink] for all Analytics Zoo and BigDL
 * pipelines. Major difference to other SparkSinks is 
 * the requirement to initiate the BigDL engine. 
 */
public class DLSink extends BaseSink {

	private static final long serialVersionUID = -5096853079445396877L;

	@Override
	public void prepareRun(SparkPluginContext context) throws Exception {
		throw new Exception("not implemented.");
		
	}

	@Override
	public void run(SparkExecutionPluginContext context, JavaRDD<StructuredRecord> input) throws Exception {

		JavaSparkContext jsc = context.getSparkContext();
		/*
		 * In case of an empty input immediately return without 
		 * any further processing
		 */
		if (input.isEmpty())
			return;

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
		 * STEP #3: Compute data model from 'rows' leveraging 
		 * the underlying Scala library of Predictive Works
		 */
		compute(context, rows);

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
