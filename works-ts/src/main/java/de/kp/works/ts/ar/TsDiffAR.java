package de.kp.works.ts.ar;
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
import de.kp.works.ts.model.DiffAutoRegressionModel;

@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("TsDiffAR")
@Description("A prediction stage that leverages a trained Apache Spark based Differencing AutoRegression time series model.")
public class TsDiffAR extends TimeCompute {

	private static final long serialVersionUID = 5008850620168692633L;

	private DiffAutoRegressionModel model;
	
	public TsDiffAR(TsDiffARConfig config) {
		this.config = config;
	}

	@Override
	public void initialize(SparkExecutionPluginContext context) throws Exception {
		
		TsDiffARConfig computeConfig = (TsDiffARConfig) config;
		computeConfig.validate();

		model = new ARManager().readDiffAR(modelFs, modelMeta, computeConfig.modelName);
		if (model == null)
			throw new IllegalArgumentException(
					String.format("[%s] A Differencing AutoRegression model with name '%s' does not exist.",
							this.getClass().getName(), computeConfig.modelName));

	}

	public static class TsDiffARConfig extends ARConfig {

		private static final long serialVersionUID = -8352931460177951709L;

		public void validate() {
			super.validate();
		}
		
	}

}
