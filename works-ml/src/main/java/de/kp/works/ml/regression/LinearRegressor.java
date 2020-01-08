package de.kp.works.ml.regression;
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

import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Strings;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import de.kp.works.core.BaseRegressorConfig;
import de.kp.works.core.BaseRegressorSink;

@Plugin(type = "sparksink")
@Name("LinearRegressor")
@Description("A building stage for an Apache Spark based linear regressor model.")
public class LinearRegressor extends BaseRegressorSink {

	private static final long serialVersionUID = -9189352556712045343L;
	
	public LinearRegressor(LinearConfig config) {
		this.config = config;
		this.className = LinearRegressor.class.getName();
	}

	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
		super.configurePipeline(pipelineConfigurer);

		/* Validate configuration */
		config.validate();
		
		/* Validate schema */
		StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
		inputSchema = stageConfigurer.getInputSchema();
		if (inputSchema != null)
			validateSchema(inputSchema, config);

	}

	public static class LinearConfig extends BaseRegressorConfig {

		private static final long serialVersionUID = -1317889959730192346L;

		public LinearConfig() {
			/*
			 * The default split of the dataset into train & test data
			 * is set to 70:30
			 */
			dataSplit = "70:30";
		}
	    
		@Override
		public Map<String, Object> getParamsAsMap() {
			
			Map<String, Object> params = new HashMap<>();
			params.put("split", dataSplit);

			return params;
		
		}

		public void validate() {

			/** MODEL & COLUMNS **/
			if (!Strings.isNullOrEmpty(modelName)) {
				throw new IllegalArgumentException("[LinearConfig] The model name must not be empty.");
			}
			if (!Strings.isNullOrEmpty(featuresCol)) {
				throw new IllegalArgumentException("[LinearConfig] The name of the field that contains the feature vector must not be empty.");
			}
			if (!Strings.isNullOrEmpty(labelCol)) {
				throw new IllegalArgumentException("[LinearConfig] The name of the field that contains the label value must not be empty.");
			}
			
			/** PARAMETERS **/
			
		}
				
	}

}
