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
import de.kp.works.ts.model.ARYuleWalkerModel;

@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("TsYuleWalker")
@Description("A prediction stage that leverages a trained Apache Spark based Yule Walker AutoRegression time series model.")
public class TsYuleWalker extends TimeCompute {

	private static final long serialVersionUID = -3512433728877952854L;

	private ARYuleWalkerModel model;
	
	public TsYuleWalker(TsYuleWalkerConfig config) {
		this.config = config;
	}

	@Override
	public void initialize(SparkExecutionPluginContext context) throws Exception {
		
		TsYuleWalkerConfig computeConfig = (TsYuleWalkerConfig) config;
		computeConfig.validate();

		model = new ARManager().readYuleWalker(modelFs, modelMeta, computeConfig.modelName);
		if (model == null)
			throw new IllegalArgumentException(
					String.format("[%s] A Yule Walker AutoRegression model with name '%s' does not exist.",
							this.getClass().getName(), computeConfig.modelName));

	}

	public static class TsYuleWalkerConfig extends ARConfig {

		private static final long serialVersionUID = -864185065637543716L;

		public void validate() {
			super.validate();
		}
		
	}

}
