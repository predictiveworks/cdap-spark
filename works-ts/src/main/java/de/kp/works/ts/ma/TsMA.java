package de.kp.works.ts.ma;
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

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import de.kp.works.core.TimeCompute;
import de.kp.works.ts.arma.ARMAConfig;
import de.kp.works.ts.model.MovingAverageModel;

@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("TsMA")
@Description("A prediction stage that leverages a trained Apache Spark based Moving Average time series model.")
public class TsMA extends TimeCompute {
	
	private static final long serialVersionUID = -1261080177077886834L;

	private MovingAverageModel model;
	
	public TsMA(TsMAConfig config) {
		this.config = config;
	}

	@Override
	public void initialize(SparkExecutionPluginContext context) throws Exception {
		
		TsMAConfig computeConfig = (TsMAConfig) config;
		computeConfig.validate();

		model = new MAManager().readMA(modelFs, modelMeta, computeConfig.modelName);
		if (model == null)
			throw new IllegalArgumentException(
					String.format("[%s] A Moving Average model with name '%s' does not exist.",
							this.getClass().getName(), computeConfig.modelName));

	}

	public static class TsMAConfig extends ARMAConfig {

		private static final long serialVersionUID = -4883049931173631393L;

		public void validate() {
			super.validate();
		}
		
	}

}
