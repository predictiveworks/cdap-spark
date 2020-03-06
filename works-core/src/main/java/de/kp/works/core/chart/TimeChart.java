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

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.google.common.base.Strings;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.dataset.table.TableProperties;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import co.cask.cdap.etl.api.batch.SparkPluginContext;
import co.cask.cdap.etl.api.batch.SparkSink;
import de.kp.works.core.BaseConfig;
import de.kp.works.core.Names;
import de.kp.works.core.Params;
import de.kp.works.core.SchemaUtil;
import de.kp.works.core.ml.sampling.LTTBuckets;
/*
 * This class is intended to be used as a base sink for plotting purposes;
 * it writes records to a Table with one record field mapping to the Table 
 * row key and all other record fields mapping to Table columns.
 */
@Plugin(type = SparkSink.PLUGIN_TYPE)
@Name("TimeChart")
@Description("A batch sink to write a time series of structured records to an internal table "
		+ "to support data visualization.")
public class TimeChart extends ChartSink {

	private static final long serialVersionUID = 8781344060041056618L;

	private TimeChartConfig config;
	private Table table;
	
	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
		super.configurePipeline(pipelineConfigurer);

		/* Validate configuration */
		config.validate();

		/* Validate schema */
		StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
		inputSchema = stageConfigurer.getInputSchema();
		if (inputSchema != null)
			validateSchema(inputSchema);


	}

	@Override
	public void prepareRun(SparkPluginContext context) throws Exception {
		/*
		 * Check whether the table with the provided table name already exists
		 */
		String tableName = config.tableName;
		if (context.datasetExists(tableName)) {
			table = context.getDataset(tableName);

		} else {

			TableProperties.Builder builder = TableProperties.builder();
			builder.setDescription(String.format(
					"This table contains a time series of metadata with time field '%s' and feature '%s'.",
					config.timeCol, config.valueCol));
			
			builder.setTTL(config.ttl);

			context.createDataset(tableName, Table.class.getName(), builder.build());
			table = context.getDataset(tableName);

		}

	}

	@Override
	public void compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {
		
		Dataset<Row> sampled = null;
		/*
		 * STEP #1: Downsample the dataset for support proper visualization; 
		 * this stage also casts the value column
		 */
		switch (config.sampling) {
		case Names.LLT_BUCKETS : {
			/*
			 * Largest Triangle Three Buckets sampling is an efficient
			 * sampling method, that cannot be used with huge datasets
			 * as sampling must be performed on the master node.
			 * 
			 * It is provided as a final sampling stage for time series
			 * visualization that may be used in combination with other
			 * sampling stages prior to this SparkSink.
			 */
			LTTBuckets sampler = new LTTBuckets();
			sampler.setXCol(config.timeCol);
			sampler.setYCol(config.valueCol);
			
			sampler.setSampleSize(config.limit);
			sampled = sampler.transform(source);
			
			break;
		}
			default:
				throw new IllegalArgumentException(String.format("Sampling method '$s' is not supported.", config.sampling));
		}
		/*
		 * STEP #2: After downsampling, the time series data (time, value)
		 * is persisted leveraging CDAP's Table API
		 */
		writeDataset(table, config.timeCol, config.valueCol, sampled);
		
	}

	@Override
	public void validateSchema(Schema inputSchema) {
		config.validateSchema(inputSchema);
	}	
	
	public static class TimeChartConfig extends BaseConfig {

		private static final long serialVersionUID = 2576131325883899416L;

		@Description("The name of the table that is used to persist time series data.")
		@Macro
		public String tableName;

		@Description(Params.TTL)
		@Macro
		public Long ttl;
		
		@Description(Params.TIME_COL)
		@Macro
		public String timeCol;

		@Description(Params.VALUE_COL)
		@Macro
		public String valueCol;
		
		@Description(Params.CHART_LIMIT)
		@Macro
		public Integer limit;
		
		@Description(Params.CHART_SAMPLING)
		@Macro
		public String sampling;
		
		public TimeChartConfig() {
			limit = 1000;
			sampling = Names.LLT_BUCKETS;
		}
		
		public void validate() {
			super.validate();

			if (Strings.isNullOrEmpty(tableName)) {
				throw new IllegalArgumentException(
						String.format("[%s] The name of the table that persist the time series data must not be empty.", this.getClass().getName()));
			}

			if (Strings.isNullOrEmpty(timeCol)) {
				throw new IllegalArgumentException(
						String.format("[%s] The name of the field that contains the timestamp must not be empty.", this.getClass().getName()));
			}
			
			if (Strings.isNullOrEmpty(valueCol)) {
				throw new IllegalArgumentException(
						String.format("[%s] The name of the field that contains the feature value must not be empty.",
								this.getClass().getName()));
			}

			if (limit < 1) {
				throw new IllegalArgumentException(
						String.format("[%s] The maximum number of data points must be positive.",
								this.getClass().getName()));
				
			}
		}

		public void validateSchema(Schema inputSchema) {

			/** TIME COLUMN **/

			Schema.Field timeField = inputSchema.getField(timeCol);
			if (timeField == null) {
				throw new IllegalArgumentException(String.format(
						"[%s] The input schema must contain the field that defines the timestamp.", this.getClass().getName()));
			}

			Schema.Type timeType = timeField.getSchema().getType();
			if (SchemaUtil.isTimeType(timeType) == false) {
				throw new IllegalArgumentException("The data type of the time value field must be LONG.");
			}

			/** VALUE COLUMN **/
				
			Schema.Field valueField = inputSchema.getField(valueCol);
			if (valueField == null) {
				throw new IllegalArgumentException(String
						.format("[%s] The input schema must contain the specified field: %s.", this.getClass().getName(), valueCol));
			}
			Schema.Type valueType = valueField.getSchema().getType();
			/*
			 * The value must be a numeric data type (double, float, int, long), which then
			 * is casted to Double
			 */
			if (SchemaUtil.isNumericType(valueType) == false) {
				throw new IllegalArgumentException(String.format("The data type of the field '%s' must be MUMERIC.", valueCol));
			}

		}
		
	}

}
