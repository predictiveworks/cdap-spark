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
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.spark.sql.DataFrames;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;

public abstract class BaseCompute extends SparkCompute<StructuredRecord, StructuredRecord> {

	private static final long serialVersionUID = 6855738584152026479L;
	/*
	 * An indicator to determine whether an optionally provided
	 * output schema must be taken into account or not
	 */
	private Boolean ignoreOutputSchema = false;
	
	public BaseCompute() {
	}
	
	public Boolean getIgnoreOutputSchema() {
		return ignoreOutputSchema;
	}
	
	public void setIgnoreOutputSchema(Boolean indicator) {
		ignoreOutputSchema = indicator;
	}
		
	/**
	 * This method is given a Spark RDD (Resilient Distributed Dataset) containing every object
	 * that is received from the previous stage. It performs Spark operations on the input to 
	 * transform it into an output RDD that will be sent to the next stage.
	 * 
	 * @param context
	 * @param input
	 * @return
	 * @throws Exception
	 */
	@Override
	public JavaRDD<StructuredRecord> transform(SparkExecutionPluginContext context, JavaRDD<StructuredRecord> input)
			throws Exception {

		JavaSparkContext jsc = context.getSparkContext();

		if (input.isEmpty()) {
			return jsc.emptyRDD();
		}

		Schema inputSchema = context.getInputSchema();

		if (inputSchema == null) {
			inputSchema = input.first().getSchema();
		}
	    
		SparkSession session = new SparkSession(jsc.sc());		

		/*
		 * STEP #1: Transform JavaRDD<StructuredRecord> into Dataset<Row> 
		 */
		StructType structType = DataFrames.toDataType(inputSchema);
		Dataset<Row> rows = SessionHelper.toDataset(input,structType,session);
		
		/*
		 * STEP #2: Compute source with underlying Scala library
		 */
		Dataset<Row> output = compute(context, rows);
		
		Schema outputSchema = context.getOutputSchema();
		
		if (outputSchema == null || ignoreOutputSchema == true) {
			outputSchema = DataFrames.toSchema(output.schema());
		}
		/*
		 * STEP #3: Transform Dataset<Row> into JavaRDD<StructuredRecord> 
		 */
		JavaRDD<StructuredRecord> records = SessionHelper.fromDataset(output, outputSchema);
		return records;
		
	}

	public Dataset<Row> compute(SparkExecutionPluginContext context,Dataset<Row> source) throws Exception {
		throw new Exception("[BaseCompute] Not implemented");
	}
}
