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

import de.kp.works.core.recording.feature.MinHashLSHRecorder;
import org.apache.spark.ml.feature.MinHashLSHModel;
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

@Plugin(type = SparkSink.PLUGIN_TYPE)
@Name("MinHashLSHBuilder")
@Description("A building stage for an Apache Spark ML MinHash LSH model.")
public class MinHashLSHBuilder extends FeatureSink {
	/*
	 * MinHash is an LSH family for Jaccard distance where input features are sets of natural numbers. 
	 * This algorithm applies a random hash function g to each element in the set and take the minimum 
	 * of all hashed values.
	 * 
	 * The input sets for MinHash are represented as binary vectors, where the vector indices represent 
	 * the elements themselves and the non-zero values in the vector represent the presence of that element 
	 * in the set. While both dense and sparse vectors are supported, typically sparse vectors are recommended 
	 * for efficiency. 
	 */
	private static final long serialVersionUID = 1064127164732936531L;

	private final MinHashLSHBuilderConfig config;
	
	public MinHashLSHBuilder(MinHashLSHBuilderConfig config) {
		this.config = config;
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
			validateSchema(inputSchema);

	}
	
	@Override
	public void validateSchema(Schema inputSchema) {
		config.validateSchema(inputSchema);
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
		String modelParams = config.getParamsAsJSON();
		String modelMetrics = new Gson().toJson(metrics);

		String modelName = config.modelName;
		String modelStage = config.modelStage;
		
		new MinHashLSHRecorder().track(context, modelName, modelStage, modelParams, modelMetrics, model);

	}

	public static class MinHashLSHBuilderConfig extends FeatureModelConfig {

		private static final long serialVersionUID = -7853341044335453501L;
		
		@Description("The number of hash tables used in LSH OR-amplification. LSH OR-amplification can be used to reduce the false negative rate. "
				+ "Higher values for this parameter lead to a reduced false negative rate, at the expense of added computational complexity. Default is 1.")
		@Macro
		public Integer numHashTables;
		
		public MinHashLSHBuilderConfig() {
			modelStage = "experiment";
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

			/* PARAMETERS */
			if (numHashTables < 1)
				throw new IllegalArgumentException(String.format(
						"[%s] The number of hash tables must be at least 1.", this.getClass().getName()));

		}
		
		public void validateSchema(Schema inputSchema) {
			super.validateSchema(inputSchema);
			
			SchemaUtil.isArrayOfNumeric(inputSchema, inputCol);
			
		}
		
	}
}
