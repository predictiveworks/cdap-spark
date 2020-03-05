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

import org.apache.spark.ml.feature.BucketedRandomProjectionLSHModel;
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
import co.cask.cdap.etl.api.batch.SparkSink;

import de.kp.works.core.SchemaUtil;
import de.kp.works.core.feature.FeatureModelConfig;
import de.kp.works.core.feature.FeatureSink;

@Plugin(type = SparkSink.PLUGIN_TYPE)
@Name("BucketedLSHBuilder")
@Description("A building stage for an Apache Spark ML Bucketed Random Projection LSH model.")
public class BucketedLSHBuilder extends FeatureSink {
	/*
	 * Bucketed Random Projection is an LSH family for Euclidean distance. 
	 * Its LSH family projects feature vectors x onto a random unit vector v 
	 * and portions the projected results into hash buckets:
	 * 
	 * 			h(x) = (x * v) / r
	 * 
	 * where r is a user-defined bucket length. The bucket length can be used 
	 * to control the average size of hash buckets (and thus the number of buckets). 
	 * 
	 * A larger bucket length (i.e., fewer buckets) increases the probability of features 
	 * being hashed to the same bucket (increasing the numbers of true and false positives).
	 * 
	 * Bucketed Random Projection accepts arbitrary vectors as input features, and supports
	 * both sparse and dense vectors.
	 */
	private static final long serialVersionUID = 4538434693829022065L;

	private BucketedLSHBuilderConfig config;
	
	public BucketedLSHBuilder(BucketedLSHBuilderConfig config) {
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
		BucketedLSHTrainer trainer = new BucketedLSHTrainer();
		Dataset<Row> vectorset = trainer.vectorize(source, featuresCol, vectorCol);

		BucketedRandomProjectionLSHModel model = trainer.train(vectorset, vectorCol, params);

		Map<String, Object> metrics = new HashMap<>();
		/*
		 * Store trained Bucketed Random Projection LSH model including its associated 
		 * parameters and metrics
		 */
		String modelParams = config.getParamsAsJSON();
		String modelMetrics = new Gson().toJson(metrics);

		String modelName = config.modelName;
		String modelStage = config.modelStage;
		
		new BucketedLSHRecorder().track(context, modelName, modelStage, modelParams, modelMetrics, model);

	}

	public static class BucketedLSHBuilderConfig extends FeatureModelConfig {

		private static final long serialVersionUID = -7853341044335453501L;
		
		@Description("The number of hash tables used in LSH OR-amplification. LSH OR-amplification can be used to reduce the false negative rate. "
				+ "Higher values for this parameter lead to a reduced false negative rate, at the expense of added computational complexity. Default is 1.")
		@Macro
		public Integer numHashTables;

		@Description("The length of each hash bucket, a larger bucket lowers the false negative rate. The number of buckets will be "
				+ "'(max L2 norm of input vectors) / bucketLength'.")
		@Macro
		public Double bucketLength;
		
		public BucketedLSHBuilderConfig() {
			modelStage = "experiment";
			numHashTables = 1;
		}
		
		@Override
		public Map<String, Object> getParamsAsMap() {

			Map<String, Object> params = new HashMap<>();
			params.put("numHashTables", numHashTables);
			params.put("bucketLength", bucketLength);

			return params;

		}
		
		public void validate() {
			super.validate();

			/** PARAMETERS **/
			if (numHashTables < 1)
				throw new IllegalArgumentException(String.format(
						"[%s] The number of hash tables must be at least 1.", this.getClass().getName()));

			if (bucketLength <= 0D)
				throw new IllegalArgumentException(String.format(
						"[%s] The bucket length  must be greater than 0.0.", this.getClass().getName()));

		}
		
		public void validateSchema(Schema inputSchema) {
			super.validateSchema(inputSchema);
			
			/** INPUT COLUMN **/
			SchemaUtil.isArrayOfNumeric(inputSchema, inputCol);
			
		}
		
	}
	
}
