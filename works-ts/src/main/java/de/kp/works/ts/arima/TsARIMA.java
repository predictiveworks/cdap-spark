package de.kp.works.ts.arima;
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

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import de.kp.works.ts.model.ARIMAModel;

@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("TsARIMA")
@Description("A transformation stage that leverages a trained ARIMA model to look n steps in time ahead. "
		+ "The forecast result is described by a two column output schema, one column specifies the future "
		+ "points in time, and another the forecasted values.")
public class TsARIMA extends ARIMACompute {

	private static final long serialVersionUID = 5020395409516031250L;

	private TsARIMAConfig config;
	private ARIMAModel model;
	
	public TsARIMA(TsARIMAConfig config) {
		this.config = config;
	}

	@Override
	public void initialize(SparkExecutionPluginContext context) throws Exception {
		
		config.validate();

		model = new ARIMARecorder().readARIMA(context, config.modelName, config.modelStage);
		if (model == null)
			throw new IllegalArgumentException(
					String.format("[%s] An ARIMA model with name '%s' does not exist.",
							this.getClass().getName(), config.modelName));

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
			outputSchema = getOutputSchema(config.timeCol, config.valueCol);
			stageConfigurer.setOutputSchema(outputSchema);

		}

	}
	
	@Override
	public Dataset<Row> compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {
		
		/* Time & value column may have names different from traing phase */
		model.setTimeCol(config.timeCol);
		model.setValueCol(config.valueCol);

		return model.forecast(source, config.steps);
		
	}

	@Override
	public void validateSchema(Schema inputSchema) {
		config.validateSchema(inputSchema);
	}

	public static class TsARIMAConfig extends ARIMAComputeConfig {

		private static final long serialVersionUID = -4483518955647431101L;

		public TsARIMAConfig() {
			modelStage = "experiment";
			steps = 1;
		}

		public void validate() {
			super.validate();
		}
		
	}
}
