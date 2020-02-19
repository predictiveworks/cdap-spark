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

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.google.common.base.Strings;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import de.kp.works.core.time.TimeCompute;
import de.kp.works.core.time.TimeConfig;

@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("TsInterpolate")
@Description("A time series interpolation stage that adds missing values. This stage interpolates missing values "
		+ "from the last non-null value before and the first on-null value after the respective null value.")
public class TsInterpolate extends TimeCompute {

	private static final long serialVersionUID = -25164752921823527L;

	private TsInterpolateConfig config;

	public TsInterpolate(TsInterpolateConfig config) {
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

		Interpolate computer = new Interpolate();
		
		String timeCol = config.timeCol;
		computer.setTimeCol(timeCol);
		
		String valueCol = config.valueCol;
		computer.setValueCol(valueCol);

		if (!Strings.isNullOrEmpty(config.groupCol))
			computer.setGroupCol(config.groupCol);

		Dataset<Row> output = computer.transform(source);
		return output;

	}

	public Schema getOutputSchema(Schema inputSchema) {
		
		List<Schema.Field> outfields = new ArrayList<>();
		for (Schema.Field field: inputSchema.getFields()) {
			/*
			 * Cast the data type of the value field to double
			 */
			if (field.getName().equals(config.valueCol)) {
				outfields.add(Schema.Field.of(config.valueCol, Schema.of(Schema.Type.DOUBLE)));
				
			} else
				outfields.add(field);
		}
		
		return Schema.recordOf(inputSchema.getRecordName(), outfields);

	}

	@Override
	public void validateSchema(Schema inputSchema) {
		config.validateSchema(inputSchema);
	}

	public static class TsInterpolateConfig extends TimeConfig {

		private static final long serialVersionUID = -833273325170246060L;

		@Description(TimeConfig.GROUP_COL_DESC)
		@Macro
		@Nullable
		public String groupCol;

		public void validate() {
			super.validate();
		}
	}

}
