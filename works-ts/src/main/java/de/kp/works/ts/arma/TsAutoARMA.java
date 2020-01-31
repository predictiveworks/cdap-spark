package de.kp.works.ts.arma;
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
import de.kp.works.ts.model.AutoARMAModel;

@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("TsAutoARMA")
@Description("A prediction stage that leverages a trained Apache Spark based Auto ARMA time series model.")
public class TsAutoARMA extends ARMACompute {

	private static final long serialVersionUID = -4782182822373835162L;

	private AutoARMAModel model;
	
	public TsAutoARMA(TsAutoARMAConfig config) {
		this.config = config;
	}

	@Override
	public void initialize(SparkExecutionPluginContext context) throws Exception {
		
		TsAutoARMAConfig computeConfig = (TsAutoARMAConfig) config;
		computeConfig.validate();

		model = new ARMAManager().readAutoARMA(modelFs, modelMeta, computeConfig.modelName);
		if (model == null)
			throw new IllegalArgumentException(
					String.format("[%s] An Auto ARMA model with name '%s' does not exist.",
							this.getClass().getName(), computeConfig.modelName));

	}
	
	@Override
	public Dataset<Row> compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {
		
		TsAutoARMAConfig computeConfig = (TsAutoARMAConfig)(config);
		
		/* Time & value column may have names different from traing phase */
		model.setTimeCol(computeConfig.timeCol);
		model.setValueCol(computeConfig.valueCol);

		return model.forecast(source, computeConfig.steps);
		
	}

	public static class TsAutoARMAConfig extends ARMAComputeConfig {

		private static final long serialVersionUID = -3952093227734706917L;

		public TsAutoARMAConfig() {
			steps = 1;
		}

		public void validate() {
			super.validate();
		}
		
	}

}
