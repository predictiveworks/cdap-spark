package de.kp.works.core;
/*
 * Copyright (c) 2018 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * This software is the confidential and proprietary information of 
 * Dr. Krusche & Partner PartG ("Confidential Information"). 
 * 
 * You shall not disclose such Confidential Information and shall use 
 * it only in accordance with the terms of the license agreement you 
 * entered into with Dr. Krusche & Partner PartG.
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
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import co.cask.cdap.etl.api.batch.SparkSink;

public abstract class BaseSink extends SparkSink<StructuredRecord> {

	private static final long serialVersionUID = -4938491756852655492L;
	
	/**
	 * This method is given a Spark RDD (Resilient Distributed Dataset) containing every object
	 * that is received from the previous stage. It performs Spark operations on the input, and
	 * usually saves the result to a dataset.
	 * 
	 * A SparkSink is similar to a SparkCompute plugin except that it has no output. In a SparkSink,
	 * you are given access to anything you would be able to do in a Spark program. For example, one 
	 * common use case is to train a machine-learning model in this plugin.
	 */
	@Override
	public void run(SparkExecutionPluginContext context, JavaRDD<StructuredRecord> input) throws Exception {

		JavaSparkContext jsc = context.getSparkContext();
		if (input.isEmpty()) return;

		Schema inputSchema = context.getInputSchema();

		if (inputSchema == null) {
			inputSchema = input.first().getSchema();
		}
	    
		SparkSession session = new SparkSession(jsc.sc());		

		/*
		 * STEP #1: Transform JavaRDD<StructuredRecord> into Dataset<Row> 
		 */
		StructType structType = DataFrames.toDataType(inputSchema);
		Dataset<Row> rows = SessionHelper.toDataset(input, structType, session);
		/*
		 * STEP #2: Compute data model from 'rows' leveraging the
		 * underlying Scala library of Predictive Works 
		 */
		compute(context, rows);

	}

	public void compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {
		throw new Exception("[ERROR] Not implemented");
	}

}
