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
@Name("TsResample")
@Description("A time series resampling stage that turns a sparse time series into an equidistant time grid. "
		+ "This stage supports data values that belong to different categories or groups, say sensor devices. "
		+ "Then resampling is performed on a per group basis. Note, resampling may lead to missing values for "
		+ "intermediate points in time.")
public class TsResample extends TimeCompute {

	private static final long serialVersionUID = 2602057748184161616L;

	private final TsResampleConfig config;

	public TsResample(TsResampleConfig config) {
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

		Resample computer = new Resample();
		
		String timeCol = config.timeCol;
		computer.setTimeCol(timeCol);
		
		String valueCol = config.valueCol;
		computer.setValueCol(valueCol);

		if (!Strings.isNullOrEmpty(config.groupCol))
			computer.setGroupCol(config.groupCol);

		computer.setStepSize(config.stepSize);
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

	public static class TsResampleConfig extends TimeConfig {

		private static final long serialVersionUID = -833273325170246060L;

		@Description(TimeConfig.GROUP_COL_DESC)
		@Macro
		@Nullable
		public String groupCol;

		@Description("Distance between subsequent points of the time series after resampling. The value "
				+ "specifies a certain time interval in seconds. Default is 30.")
		@Macro
		public Integer stepSize;

		public TsResampleConfig() {
			stepSize = 30;
		}
		
		public void validate() {
			super.validate();

			/* PARAMETERS */
			if (stepSize < 1)
				throw new IllegalArgumentException(
						String.format("[%s] The time interval between neighboring points must be at least 1 second.", this.getClass().getName()));
			

		}
	}

}
