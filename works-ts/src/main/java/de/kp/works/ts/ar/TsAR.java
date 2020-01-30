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

import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import de.kp.works.core.TimeCompute;
import de.kp.works.ts.model.AutoRegressionModel;

public class TsAR extends TimeCompute {

	private static final long serialVersionUID = -4388615366081416402L;

	private AutoRegressionModel model;
	
	public TsAR(TsARConfig config) {
		this.config = config;
	}

	@Override
	public void initialize(SparkExecutionPluginContext context) throws Exception {
		
		TsARConfig computeConfig = (TsARConfig) config;
		computeConfig.validate();

		model = new ARManager().readAR(modelFs, modelMeta, computeConfig.modelName);
		if (model == null)
			throw new IllegalArgumentException(
					String.format("[%s] An AutoRegression model with name '%s' does not exist.",
							this.getClass().getName(), computeConfig.modelName));

	}

	public static class TsARConfig extends ARConfig {

		private static final long serialVersionUID = 7633572327423290491L;

		public void validate() {
			super.validate();
		}
		
	}
}
