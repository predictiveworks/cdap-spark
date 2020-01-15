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

import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.google.gson.Gson;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import de.kp.works.core.BaseFeatureModelConfig;
import de.kp.works.core.BaseFeatureSink;

@Plugin(type = "sparksink")
@Name("StringIndexerBuilder")
@Description("A building stage for an Apache Spark based String Indexer model.")
public class StringIndexerBuilder extends BaseFeatureSink {
	/*
	 * A label indexer that maps a string column of labels to an ML column of label indices.
	 * If the input column is numeric, we cast it to string and index the string values.
	 * The indices are in [0, numLabels), ordered by label frequencies.
	 * So the most frequent label gets index 0.
	 */
	private static final long serialVersionUID = -2360022873735403321L;

	public StringIndexerBuilder(StringIndexerBuilderConfig config) {
		this.config = config;
		this.className = StringIndexerBuilder.class.getName();
	}

	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
		super.configurePipeline(pipelineConfigurer);

		/* Validate configuration */
		((StringIndexerBuilderConfig)config).validate();

		StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();

		inputSchema = stageConfigurer.getInputSchema();
		if (inputSchema != null)
			validateSchema(inputSchema, config);

	}

	@Override
	public void compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {

		StringIndexerBuilderConfig builderConfig = (StringIndexerBuilderConfig)config;

		org.apache.spark.ml.feature.StringIndexer trainer = new org.apache.spark.ml.feature.StringIndexer();
		trainer.setInputCol(builderConfig.inputCol);

		StringIndexerModel model = trainer.fit(source);
		
		Map<String, Object> metrics = new HashMap<>();
		/*
		 * Store trained StringIndexer model including its associated
		 * parameters and metrics
		 */
		String paramsJson = builderConfig.getParamsAsJSON();
		String metricsJson = new Gson().toJson(metrics);

		String modelName = config.modelName;
		new StringIndexerManager().save(modelFs, modelMeta, modelName, paramsJson, metricsJson, model);
		
	}
	
	@Override
	public void validateSchema(Schema inputSchema, BaseFeatureModelConfig config) {
		super.validateSchema(inputSchema, config);
		
		/** INPUT COLUMN **/
		isString(config.inputCol);
		
	}

	public static class StringIndexerBuilderConfig extends BaseFeatureModelConfig {

		private static final long serialVersionUID = -97589053635760766L;
		
		public void validate() {
			super.validate();
		}
	}
}
