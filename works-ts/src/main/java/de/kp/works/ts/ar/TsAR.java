package de.kp.works.ts.ar;
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

import de.kp.works.ts.recording.ARRecorder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.SparkCompute;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;

import de.kp.works.ts.model.AutoRegressionModel;

@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("TsAR")
@Description("A prediction stage that leverages a trained AutoRegression (AR) time series model. "
		+ "to look n steps in time ahead. The forecast result is described by a two column output "
		+ "schema, one column specifies the future points in time, and another the forecasted values.")
public class TsAR extends ARCompute {

	private static final long serialVersionUID = -4388615366081416402L;

	private final TsARConfig config;
	private AutoRegressionModel model;
	
	public TsAR(TsARConfig config) {
		this.config = config;
	}

	@Override
	public void initialize(SparkExecutionPluginContext context) throws Exception {
		
		config.validate();
		
		ARRecorder recorder = new ARRecorder(configReader);
		/* 
		 * STEP #1: Retrieve the trained regression model
		 * that refers to the provide name, stage and option
		 */
		model = recorder.readAR(context, config.modelName, config.modelStage, config.modelOption);
		if (model == null)
			throw new IllegalArgumentException(
					String.format("[%s] An AutoRegression model with name '%s' does not exist.",
							this.getClass().getName(), config.modelName));

		/* 
		 * STEP #2: Retrieve the profile of the trained
		 * regression model for subsequent annotation
		 */
		profile = recorder.getProfile();

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
			 * output schema by explicitly adding the prediction column
			 */
			outputSchema = getOutputSchema(config.timeCol, config.valueCol, STATUS_FIELD);
			stageConfigurer.setOutputSchema(outputSchema);

		}

	}
	
	@Override
	public Dataset<Row> compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {
		
		/* Time & value column may have names different from traing phase */
		model.setTimeCol(config.timeCol);
		model.setValueCol(config.valueCol);

		Dataset<Row> forecast = model.forecast(source, config.steps);
		return assembleAndAnnotate(source, forecast, config.timeCol, config.valueCol);
		
	}

	@Override
	public void validateSchema(Schema inputSchema) {
		config.validateSchema(inputSchema);
	}

	public static class TsARConfig extends ARComputeConfig {

		private static final long serialVersionUID = 7633572327423290491L;

		public TsARConfig() {

			modelOption = BEST_MODEL;
			modelStage = "experiment";
			
			steps = 1;

		}

		public void validate() {
			super.validate();
		}
		
	}
}
