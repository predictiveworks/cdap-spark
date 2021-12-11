package de.kp.works.ts;
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

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.google.common.base.Strings;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.SparkCompute;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import de.kp.works.core.time.TimeCompute;
import de.kp.works.core.time.TimeConfig;

@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("TsAggregate")
@Description("A time series aggregation stage that aggregates a sparse time series leveraging a user-defined "
		+ "tumbling window. Suppose a certain point in time refers to 09:00 am and a window of 10 minutes is "
		+ "defined, then all points in time falling into the window [09:00, 09:10] are collected and their "
		+ "associated values aggregated.")
public class TsAggregate extends TimeCompute {
	/*
	 * TsAggregated refers to the preprocessing phase of time series
	 * processing and is an alternative to transforming a series into
	 * a time grid of equidistant points in time.
	 */
	private static final long serialVersionUID = 404643815476832744L;

	private final TsAggregateConfig config;

	public TsAggregate(TsAggregateConfig config) {
		this.config = config;
	}

	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {

		config.validate();

		StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
		/*
		 * Try to determine input and output schema; if these schemas are not explicitly
		 * specified, they will be inferred from the provided data records
		 */
		inputSchema = stageConfigurer.getInputSchema();
		if (inputSchema != null) {
			validateSchema(inputSchema);

			outputSchema = getOutputSchema(inputSchema);
			stageConfigurer.setOutputSchema(outputSchema);

		}

	}

	@Override
	public Dataset<Row> compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {

		Aggregate computer = new Aggregate();

		computer.setTimeCol(config.timeCol);
		computer.setValueCol(config.valueCol);

		if (!Strings.isNullOrEmpty(config.groupCol))
			computer.setGroupCol(config.groupCol);

		computer.setWindowDuration(config.windowDuration);
		computer.setAggregationMethod(config.aggregationMethod);

		return computer.transform(source);

	}

	public Schema getOutputSchema(Schema inputSchema) {
		
		List<Schema.Field> outfields = new ArrayList<>();
		assert inputSchema.getFields() != null;
		for (Schema.Field field: inputSchema.getFields()) {
			/*
			 * Cast the data type of the value field to double
			 */
			if (field.getName().equals(config.valueCol)) {
				outfields.add(Schema.Field.of(config.valueCol, Schema.of(Schema.Type.DOUBLE)));
				
			} else
				outfields.add(field);
		}

		assert inputSchema.getRecordName() != null;
		return Schema.recordOf(inputSchema.getRecordName(), outfields);

	}

	@Override
	public void validateSchema(Schema inputSchema) {
		config.validateSchema(inputSchema);
	}
	
	public static class TsAggregateConfig extends TimeConfig {

		private static final long serialVersionUID = -8785851598214457493L;

		@Description(TimeConfig.GROUP_COL_DESC)
		@Macro
		@Nullable
		public String groupCol;

		@Description("The time window used to aggregate intermediate values. Default is '10 minutes'.")
		@Macro
		public String windowDuration;

		@Description("The name of the aggregation method. Supported values are 'avg', 'count', 'mean' and 'sum'. Default is 'avg'.")
		@Macro
		public String aggregationMethod;

		public TsAggregateConfig() {

			aggregationMethod = "avg";
			windowDuration = "10 minutes";

		}

		public void validate() {
			super.validate();

			if (Strings.isNullOrEmpty(windowDuration)) {
				throw new IllegalArgumentException(
						String.format("[%s] The window duration must not be empty.", this.getClass().getName()));
			}

		}

	}

}
