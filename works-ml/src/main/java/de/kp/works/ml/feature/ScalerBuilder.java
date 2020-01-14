package de.kp.works.ml.feature;
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

import javax.annotation.Nullable;

import org.apache.spark.ml.feature.MaxAbsScaler;
import org.apache.spark.ml.feature.MaxAbsScalerModel;
import org.apache.spark.ml.feature.MinMaxScaler;
import org.apache.spark.ml.feature.MinMaxScalerModel;
import org.apache.spark.ml.feature.StandardScaler;
import org.apache.spark.ml.feature.StandardScalerModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.google.gson.Gson;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import de.kp.works.core.BaseFeatureModelConfig;
import de.kp.works.core.BaseFeatureSink;
import de.kp.works.ml.MLUtils;

@Plugin(type = "sparksink")
@Name("ScalerBuilder")
@Description("A building stage for an Apache Spark based feature scaling model. Supported models are Min-Max, Max-Abs and Standard Scaler.")
public class ScalerBuilder extends BaseFeatureSink {

	private static final long serialVersionUID = -7301919602186472418L;

	public ScalerBuilder(ScalerBuilderConfig config) {
		this.config = config;
		this.className = ScalerBuilder.class.getName();
	}

	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
		super.configurePipeline(pipelineConfigurer);

		/* Validate configuration */
		((ScalerBuilderConfig)config).validate();

		StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();

		inputSchema = stageConfigurer.getInputSchema();
		if (inputSchema != null)
			validateSchema(inputSchema, config);

	}
	
	@Override
	public void validateSchema(Schema inputSchema, BaseFeatureModelConfig config) {
		super.validateSchema(inputSchema, config);
		
		/** INPUT COLUMN **/
		isArrayOfNumeric(config.inputCol);
		
	}

	@Override
	public void compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {
		
		ScalerBuilderConfig scalerConfig = (ScalerBuilderConfig)config;
		/*
		 * Build internal column from input column and cast to 
		 * double vector
		 */
		Dataset<Row> vectorset = MLUtils.vectorize(source, scalerConfig.inputCol, "_input", true);
		Map<String, Object> metrics = new HashMap<>();
		/*
		 * Store trained Word2Vec model including its associated 
		 * parameters and metrics
		 */
		String paramsJson = scalerConfig.getParamsAsJSON();
		String metricsJson = new Gson().toJson(metrics);
		
		String modelType = scalerConfig.modelType;
		if (modelType.equals("minmax")) {
			
			MinMaxScaler minMaxScaler = new MinMaxScaler();
			minMaxScaler.setInputCol("_input");
		
			minMaxScaler.setMin(scalerConfig.min);
			minMaxScaler.setMax(scalerConfig.max);
			
			MinMaxScalerModel model = minMaxScaler.fit(vectorset);

			String modelName = scalerConfig.modelName;
			new ScalerManager().saveMinMaxScaler(modelFs, modelMeta, modelName, paramsJson, metricsJson, model);
			
			
		} else if (modelType.equals("maxabs")) {

			MaxAbsScaler maxAbsScaler = new MaxAbsScaler();
			maxAbsScaler.setInputCol("_input");
			
			MaxAbsScalerModel model = maxAbsScaler.fit(vectorset);

			String modelName = scalerConfig.modelName;
			new ScalerManager().saveMaxAbsScaler(modelFs, modelMeta, modelName, paramsJson, metricsJson, model);
			
		} else {
			
			StandardScaler standardScaler = new StandardScaler();
			standardScaler.setInputCol("_input");
			
			if (scalerConfig.withMean.equals("false"))
				standardScaler.setWithMean(false);
			
			else 
				standardScaler.setWithMean(true);
			
			if (scalerConfig.withStd.equals("false"))
				standardScaler.setWithStd(false);
			
			else 
				standardScaler.setWithStd(true);
				
			StandardScalerModel model = standardScaler.fit(vectorset);

			String modelName = scalerConfig.modelName;
			new ScalerManager().saveStandardScaler(modelFs, modelMeta, modelName, paramsJson, metricsJson, model);

		}
	}

	public static class ScalerBuilderConfig extends BaseFeatureModelConfig {

		private static final long serialVersionUID = -5884293794692689132L;
		
		@Description("The type of the scaler model. Supported values are 'maxabs', 'minmax' and 'standard'. Default is 'standard'.")
		@Macro
		public String modelType;
		
		@Description("The lower bound of the feature range after transformation. This parameter is "
				+ "restricted to the model type 'minmax'. Default is 0.0.")
		@Macro
		@Nullable
		public Double min;
		
		@Description("The upper bound of the feature range after transformation. This parameter is "
				+ "restricted to the model type 'minmax'. Default is 1.0.")
		@Macro
		@Nullable
		public Double max;

		@Description("Indicator to determine whether to center the data with mean before scaling. "
				+ " This parameter applies to the model type 'standard'. Default is 'false'.")
		@Macro
		@Nullable
		public String withMean;

		@Description("Indicator to determine whether to scale the data to unit standard deviation. "
				+ " This parameter applies to the model type 'standard'. Default is 'true'.")
		@Macro
		@Nullable
		public String withStd;
		
		public ScalerBuilderConfig() {
			
			modelType = "standard";
			
			min = 0.0;
			max = 1.0;
			
			withMean = "false";
			withStd  = "true";
			
		}
	    
		@Override
		public Map<String, Object> getParamsAsMap() {
			
			Map<String, Object> params = new HashMap<>();
			params.put("modelType", modelType);

			params.put("min", min);
			params.put("max", max);

			params.put("withMean", withMean);
			params.put("withStd", withStd);

			return params;
		
		}

		public void validate() {
			super.validate();
			
			if (modelType.equals("minmax") && min > max) {
				throw new IllegalArgumentException(String
						.format("[%s] The lower bound must be smaller or equal than the upper one.", this.getClass().getName()));
			}

		}
		
	}
}
