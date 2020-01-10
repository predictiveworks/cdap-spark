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

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.batch.SparkCompute;
import de.kp.works.core.BaseTimeCompute;
import de.kp.works.core.BaseTimeConfig;

@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("TsInterpolate")
@Description("A timeseries interpolation stage that adds missing values. This stage interpolates missing values "
		+ "from the last non-null value before and the first on-null value after the respective null value.")

public class TsInterpolate extends BaseTimeCompute {

	private static final long serialVersionUID = -25164752921823527L;

	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {

		TsInterpolateConfig timeConfig = (TsInterpolateConfig)config;
		timeConfig.validate();

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
			outputSchema = getOutputSchema(inputSchema, config.valueCol);
			stageConfigurer.setOutputSchema(outputSchema);

		}

	}

	public static class TsInterpolateConfig extends BaseTimeConfig {

		private static final long serialVersionUID = -833273325170246060L;

		@Description("The name of the field in the input schema that contains the group value.")
		@Macro
		@Nullable
		public String groupCol;

		public void validate() {
		}
	}

}
