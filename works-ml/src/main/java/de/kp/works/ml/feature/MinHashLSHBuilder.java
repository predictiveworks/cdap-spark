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

import org.apache.spark.ml.feature.MinHashLSHModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.google.gson.Gson;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import de.kp.works.core.BaseFeatureModelConfig;
import de.kp.works.core.BaseFeatureSink;

public class MinHashLSHBuilder extends BaseFeatureSink {

	private static final long serialVersionUID = 1064127164732936531L;

	public MinHashLSHBuilder(MinHashLSHBuilderConfig config) {
		this.config = config;
		this.className = MinHashLSHBuilder.class.getName();
	}

	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
		super.configurePipeline(pipelineConfigurer);

		/* Validate configuration */
		((MinHashLSHBuilderConfig)config).validate();

		/* Validate schema */
		StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
		inputSchema = stageConfigurer.getInputSchema();
		if (inputSchema != null)
			validateSchema(inputSchema, config);

	}
	
	@Override
	public void validateSchema(Schema inputSchema, BaseFeatureModelConfig config) {
		super.validateSchema(inputSchema, config);
		
		/** INPUT COLUMN **/
		isArrayOfDouble(config.inputCol);
		
	}

	@Override
	public void compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {

		String featuresCol = config.inputCol;
		Map<String, Object> params = config.getParamsAsMap();
		/*
		 * The vectorCol specifies the internal column that has to be built from the
		 * featuresCol and that is used for training purposes
		 */
		String vectorCol = "_vector";
		/*
		 * Prepare provided dataset by vectorizing the feature column which is specified
		 * as Array[Double]
		 */
		MinHashLSHTrainer trainer = new MinHashLSHTrainer();
		Dataset<Row> vectorset = trainer.vectorize(source, featuresCol, vectorCol);

		MinHashLSHModel model = trainer.train(vectorset, vectorCol, params);

		Map<String, Object> metrics = new HashMap<>();
		/*
		 * Store trained MinHash LSH model including its associated parameters and
		 * metrics
		 */
		String paramsJson = config.getParamsAsJSON();
		String metricsJson = new Gson().toJson(metrics);

		String modelName = config.modelName;
		new MinHashLSHManager().save(modelFs, modelMeta, modelName, paramsJson, metricsJson, model);

	}

	public static class MinHashLSHBuilderConfig extends BaseFeatureModelConfig {

		private static final long serialVersionUID = -7853341044335453501L;
		
		@Description("The number of hash tables used in LSH OR-amplification. LSH OR-amplification can be used to reduce the false negative rate. "
				+ "Higher values for this parameter lead to a reduced false negative rate, at the expense of added computational complexity. Default is 1.")
		@Macro
		public Integer numHashTables;
		
		public MinHashLSHBuilderConfig() {
			numHashTables = 1;
		}

		@Override
		public Map<String, Object> getParamsAsMap() {

			Map<String, Object> params = new HashMap<>();
			params.put("numHashTables", numHashTables);

			return params;

		}
		
		public void validate() {
			super.validate();

			/** PARAMETERS **/
			if (numHashTables < 1)
				throw new IllegalArgumentException(String.format(
						"[%s] The number of hash tables must be at least 1.", this.getClass().getName()));

		}
		
	}
}
