package de.kp.works.ml.feature;
/*
 * Copyright (c) 2019 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
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

import de.kp.works.core.recording.feature.VectorIndexerRecorder;
import org.apache.spark.ml.feature.VectorIndexerModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.google.gson.Gson;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import io.cdap.cdap.etl.api.batch.SparkSink;

import de.kp.works.core.SchemaUtil;
import de.kp.works.core.feature.FeatureModelConfig;
import de.kp.works.core.feature.FeatureSink;
import de.kp.works.core.recording.MLUtils;

@Plugin(type = SparkSink.PLUGIN_TYPE)
@Name("VectorIndexerBuilder")
@Description("A building stage for an Apache Spark ML Vector Indexer model.")
public class VectorIndexerBuilder extends FeatureSink {

	private static final long serialVersionUID = -2349583466809428065L;

	private final VectorIndexerBuilderConfig config;
	
	public VectorIndexerBuilder(VectorIndexerBuilderConfig config) {
		this.config = config;
	}

	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
		super.configurePipeline(pipelineConfigurer);

		/* Validate configuration */
		config.validate();

		StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();

		inputSchema = stageConfigurer.getInputSchema();
		if (inputSchema != null)
			validateSchema(inputSchema);

	}

	@Override
	public void compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {

		/*
		 * Build internal column from input column and cast to 
		 * double vector
		 */
		Dataset<Row> vectorset = MLUtils.vectorize(source, config.inputCol, "_input", true);

		org.apache.spark.ml.feature.VectorIndexer trainer = new org.apache.spark.ml.feature.VectorIndexer();
		trainer.setInputCol("_input");

		VectorIndexerModel model = trainer.fit(vectorset);
		
		Map<String, Object> metrics = new HashMap<>();
		/*
		 * Store trained StringIndexer model including its associated
		 * parameters and metrics
		 */
		String modelParams = config.getParamsAsJSON();
		String modelMetrics = new Gson().toJson(metrics);

		String modelName = config.modelName;
		String modelStage = config.modelStage;
		
		new VectorIndexerRecorder(configReader)
				.track(context, modelName, modelStage, modelParams, modelMetrics, model);
		
	}

	@Override
	public void validateSchema(Schema inputSchema) {
		config.validateSchema(inputSchema);
	}

	public static class VectorIndexerBuilderConfig extends FeatureModelConfig {

		private static final long serialVersionUID = 6467486002712190478L;

		@Description("The threshold for the number of values a categorical feature can take. If a feature is found "
				+ "to have more category values than this threshold, then it is declared continuous. Must be greater "
				+ "than or equal to 2. Default is 20.")
		@Macro
		public Integer maxCategories;

		public VectorIndexerBuilderConfig() {
			modelStage = "experiment";
			maxCategories = 20;
		}
		
		@Override
		public Map<String, Object> getParamsAsMap() {

			Map<String, Object> params = new HashMap<>();
			params.put("maxCategories", maxCategories);

			return params;

		}

		public void validate() {
			super.validate();

			if (maxCategories < 2) {
				throw new IllegalArgumentException(String.format(
						"[%s] The number of feature categories must be greater than 1.", this.getClass().getName()));
			}
		}
		
		public void validateSchema(Schema inputSchema) {
			super.validateSchema(inputSchema);
			
			SchemaUtil.isArrayOfNumeric(inputSchema, inputCol);
			
		}

	}
}
