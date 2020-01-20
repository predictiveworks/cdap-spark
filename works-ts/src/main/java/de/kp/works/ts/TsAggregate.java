package de.kp.works.ts;
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

import javax.annotation.Nullable;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.google.common.base.Strings;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import de.kp.works.core.TimeCompute;
import de.kp.works.core.TimeConfig;

@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("TsAggregate")
@Description("A time series aggregation stage that leverages a tumbling window.")
public class TsAggregate extends TimeCompute {

	private static final long serialVersionUID = 404643815476832744L;

	private TsAggregateConfig config;

	public TsAggregate(TsAggregateConfig config) {
		this.config = config;
	}

	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {

		((TsAggregateConfig) config).validate();

		StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
		/*
		 * Try to determine input and output schema; if these schemas are not explicitly
		 * specified, they will be inferred from the provided data records
		 */
		inputSchema = stageConfigurer.getInputSchema();
		if (inputSchema != null) {
			/*
			 * In cases where the input schema is explicitly provided, we determine the
			 * output schema and change the data type of the value field to DOUBLE
			 */
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
		
		Dataset<Row> output = computer.transform(source);
		return output;

	}

	public static class TsAggregateConfig extends TimeConfig {

		private static final long serialVersionUID = -8785851598214457493L;

		@Description("The name of the field in the input schema that contains the group value.")
		@Macro
		@Nullable
		public String groupCol;

		@Description("The time window used to aggregate intermediate values. Default is 10 minutes.")
		@Macro
		public String windowDuration;

		@Description("The name of the aggregation method. Supported values are 'avg', 'mean' and 'sum'. Default is 'avg'.")
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
