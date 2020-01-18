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

import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.spark.sql.DataFrames;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import co.cask.cdap.etl.api.batch.SparkPluginContext;
import de.kp.works.core.ml.SparkMLManager;

public class RegressorSink extends BaseSink {

	private static final long serialVersionUID = 8807040807659351717L;

	protected FileSet modelFs;
	protected Table modelMeta;
	
	protected RegressorConfig config;

	@Override
	public void prepareRun(SparkPluginContext context) throws Exception {
		/*
		 * Regression model components and metadata are persisted in a CDAP FileSet
		 * as well as a Table; at this stage, we have to make sure that these internal
		 * metadata structures are present
		 */
		SparkMLManager.createRegressionIfNotExists(context);
		/*
		 * Retrieve regression specified dataset for later use incompute
		 */
		modelFs = SparkMLManager.getRegressionFS(context);
		modelMeta = SparkMLManager.getRegressionMeta(context);
		
	}

	@Override
	public void run(SparkExecutionPluginContext context, JavaRDD<StructuredRecord> input) throws Exception {

		JavaSparkContext jsc = context.getSparkContext();
		/*
		 * In case of an empty input immediately return 
		 * without any further processing
		 */
		if (input.isEmpty())
			return;

		if (inputSchema == null) {
			
			inputSchema = input.first().getSchema();
			validateSchema(inputSchema, config);
		}

		SparkSession session = new SparkSession(jsc.sc());

		/*
		 * STEP #1: Transform JavaRDD<StructuredRecord> into Dataset<Row>
		 */
		StructType structType = DataFrames.toDataType(inputSchema);
		Dataset<Row> rows = SessionHelper.toDataset(input, structType, session);
		/*
		 * STEP #2: Compute data model from 'rows' leveraging the underlying Scala
		 * library of Predictive Works
		 */
		compute(context, rows);

	}

	public void validateSchema(Schema inputSchema, RegressorConfig config) {

		/** FEATURES COLUMN **/

		Schema.Field featuresCol = inputSchema.getField(config.featuresCol);
		if (featuresCol == null) {
			throw new IllegalArgumentException(String.format(
					"[%s] The input schema must contain the field that defines the feature vector.", className));
		}

		isArrayOfNumeric(config.featuresCol);

		/** LABEL COLUMN **/

		Schema.Field labelCol = inputSchema.getField(config.labelCol);
		if (labelCol == null) {
			throw new IllegalArgumentException(String
					.format("[%s] The input schema must contain the field that defines the label value.", className));
		}

		Schema.Type labelType = labelCol.getSchema().getType();
		/*
		 * The label must be a numeric data type (double, float, int, long), which then
		 * is casted to Double (see classification trainer)
		 */
		if (isNumericType(labelType) == false) {
			throw new IllegalArgumentException("The data type of the label field must be numeric.");
		}
	}

}
