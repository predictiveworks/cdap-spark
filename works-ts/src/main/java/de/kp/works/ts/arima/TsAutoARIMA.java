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
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import de.kp.works.ts.model.AutoARIMAModel;

@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("TsAutoARIMA")
@Description("A prediction stage that leverages a trained Apache Spark based Auto ARIMA time series model.")
public class TsAutoARIMA extends ARIMACompute {

	private static final long serialVersionUID = -1031029606032986435L;

	private AutoARIMAModel model;
	
	public TsAutoARIMA(TsAutoARIMAConfig config) {
		this.config = config;
	}

	@Override
	public void initialize(SparkExecutionPluginContext context) throws Exception {
		
		TsAutoARIMAConfig computeConfig = (TsAutoARIMAConfig) config;
		computeConfig.validate();

		model = new ARIMAManager().readAutoARIMA(context, computeConfig.modelName);
		if (model == null)
			throw new IllegalArgumentException(
					String.format("[%s] An Auto ARIMA model with name '%s' does not exist.",
							this.getClass().getName(), computeConfig.modelName));

	}
	
	@Override
	public Dataset<Row> compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {
		
		TsAutoARIMAConfig computeConfig = (TsAutoARIMAConfig)(config);
		
		/* Time & value column may have names different from traing phase */
		model.setTimeCol(computeConfig.timeCol);
		model.setValueCol(computeConfig.valueCol);

		return model.forecast(source, computeConfig.steps);
		
	}

	public static class TsAutoARIMAConfig extends ARIMAComputeConfig {

		private static final long serialVersionUID = -1122459750687688824L;

		public TsAutoARIMAConfig() {
			steps = 1;
		}

		public void validate() {
			super.validate();
		}
		
	}

}
