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

import org.apache.spark.ml.feature.ChiSqSelectorModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.google.common.base.Strings;
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
import de.kp.works.core.FeatureSink;
import de.kp.works.ml.MLUtils;

@Plugin(type = "sparksink")
@Name("ChiSquaredBuilder")
@Description("A building stage for an Apache Spark based Chi-Squared Selector model.")
public class ChiSquaredBuilder extends FeatureSink {

	private static final long serialVersionUID = -5551497359106054161L;

	public ChiSquaredBuilder(ChiSquaredBuilderConfig config) {
		this.config = config;
		this.className = ChiSquaredBuilder.class.getName();
	}

	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
		super.configurePipeline(pipelineConfigurer);

		/* Validate configuration */
		((ChiSquaredBuilderConfig)config).validate();

		/* Validate schema */
		StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
		inputSchema = stageConfigurer.getInputSchema();
		if (inputSchema != null)
			validateSchema(inputSchema, config);

	}
	
	@Override
	public void validateSchema(Schema inputSchema, BaseFeatureModelConfig config) {
		super.validateSchema(inputSchema, config);
		
		ChiSquaredBuilderConfig builderConfig = (ChiSquaredBuilderConfig)config;

		/** INPUT COLUMN **/
		isArrayOfNumeric(builderConfig.inputCol);

		/** Label COLUMN **/
		isNumeric(builderConfig.labelCol);
		
	}

	@Override
	public void compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {

		ChiSquaredBuilderConfig builderConfig = (ChiSquaredBuilderConfig)config;
		/*
		 * Build internal column from input column and cast to 
		 * double vector
		 */
		Dataset<Row> vectorset = MLUtils.vectorize(source, builderConfig.inputCol, "_input", true);

		org.apache.spark.ml.feature.ChiSqSelector trainer = new org.apache.spark.ml.feature.ChiSqSelector();
		trainer.setFeaturesCol("_input");
		trainer.setLabelCol(builderConfig.labelCol);

		trainer.setSelectorType(builderConfig.selectorType);
		trainer.setNumTopFeatures(builderConfig.numTopFeatures);
		
		trainer.setPercentile(builderConfig.percentile);
		trainer.setFpr(builderConfig.fpr);
		
		ChiSqSelectorModel model = trainer.fit(vectorset);
		
		Map<String, Object> metrics = new HashMap<>();
		/*
		 * Store trained StringIndexer model including its associated
		 * parameters and metrics
		 */
		String paramsJson = builderConfig.getParamsAsJSON();
		String metricsJson = new Gson().toJson(metrics);

		String modelName = config.modelName;
		new ChiSquaredManager().save(modelFs, modelMeta, modelName, paramsJson, metricsJson, model);
		
	}

	public static class ChiSquaredBuilderConfig extends BaseFeatureModelConfig {

		private static final long serialVersionUID = 2325925067724294126L;

		@Description("The name of the field in the input schema that contains the label.")
		@Macro
		public String labelCol;
		
		@Description("The number of features that selector will select, ordered by ascending p-value. "
				+ "number of features is less than this parameter value, then this will select all features. "
				+ "Only applicable when selectorType = 'numTopFeatures'. Default value is 50.")
		@Macro
		public Integer numTopFeatures;

		@Description("Percentile of features that selector will select, ordered by statistics value descending. "
				+ "Only applicable when selectorType = 'percentile'. Must be in range (0, 1). Default value is 0.1.")
		@Macro
		public Double percentile;

		@Description("The highest p-value for features to be kept. Only applicable when selectorType = 'fpr'. "
				+ "Must be in range (0, 1). Default value is 0.05.")
		@Macro
		public Double fpr;

		@Description("The selector type of the ChisqSelector. Supported values: 'numTopFeatures, 'percentile, and 'fpr'"
				+ "Default is 'numTopFeatures'.")
		@Macro
		public String selectorType;

		public ChiSquaredBuilderConfig() {
			numTopFeatures = 50;
			percentile = 0.1;
			fpr = 0.05;
			selectorType = "numTopFeatures";
		}
	    
		@Override
		public Map<String, Object> getParamsAsMap() {
			
			Map<String, Object> params = new HashMap<>();

			params.put("numTopFeatures", numTopFeatures);
			params.put("percentile", percentile);

			params.put("fpr", fpr);
			params.put("selectorType", selectorType);

			return params;
		
		}
		
		public void validate() {
			super.validate();

			if (Strings.isNullOrEmpty(labelCol)) {
				throw new IllegalArgumentException(
						String.format("[%s] The name of the field that contains the label value must not be empty.",
								this.getClass().getName()));
			}
			if (selectorType.equals("numTopFeatures") && numTopFeatures <= 1)
				throw new IllegalArgumentException(String.format(
						"[%s] The number of feature to select must be greater than 0.", this.getClass().getName()));

			if (selectorType.equals("percentile") && !(percentile > 0 && percentile < 1))
				throw new IllegalArgumentException(String.format(
						"[%s] The percentile features to select must be in range (0, 1).", this.getClass().getName()));

			if (selectorType.equals("fpr") && !(fpr > 0 && fpr < 1))
				throw new IllegalArgumentException(String.format(
						"[%s] The highest p-value for features to select must be in range (0, 1).", this.getClass().getName()));

		}
		
	}
}
