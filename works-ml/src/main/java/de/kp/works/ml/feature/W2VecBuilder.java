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

import de.kp.works.core.recording.feature.W2VecRecorder;
import org.apache.spark.ml.feature.Word2VecModel;
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
@Name("W2VecBuilder")
@Description("A building stage for an Apache Spark ML Word2Vec feature model.")
public class W2VecBuilder extends FeatureSink {

	private static final long serialVersionUID = 3885087751281049601L;

	private final W2VecBuilderConfig config;
	
	public W2VecBuilder(W2VecBuilderConfig config) {
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
			/*
			 * Check whether the input columns is of data type
			 * Array[String]
			 */
			validateSchema(inputSchema);

	}

	@Override
	public void compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {

		Map<String, Object> params = config.getParamsAsMap();

		W2VecTrainer trainer = new W2VecTrainer();
		Word2VecModel model = trainer.train(source, config.inputCol, params);

		Map<String, Object> metrics = new HashMap<>();
		/*
		 * Store trained Word2Vec model including its associated 
		 * parameters and metrics
		 */
		String modelParams = config.getParamsAsJSON();
		String modelMetrics = new Gson().toJson(metrics);

		String modelName = config.modelName;
		String modelStage = config.modelStage;
		
		new W2VecRecorder().track(context, modelName, modelStage, modelParams, modelMetrics, model);

	}
	
	@Override
	public void validateSchema(Schema inputSchema) {
		config.validateSchema(inputSchema);
	}

	public static class W2VecBuilderConfig extends FeatureModelConfig {

		private static final long serialVersionUID = 7193399365383549664L;

		@Description("The maximum number of iterations to train the Word-to-Vector model. Default is 1.")
		@Macro
		public Integer maxIter;
		
		@Description("The learning rate for shrinking the contribution of each estimator. Must be in interval (0, 1]. Default is 0.025.")
		@Macro
		public Double stepSize;

		@Description("The positive dimension of the feature vector to represent a certain word. Default is 100.")
		@Macro
		public Integer vectorSize;

		@Description("The positive window size. Default is 5.")
		@Macro
		public Integer windowSize;

		@Description("The minimum number of times a word must appear to be included in the Word-to-Vector vocabulary. Default is 5.")
		@Macro
		public Integer minCount;

		@Description("The maximum length (in words) of each sentence in the input data. Any sentence longer than this threshold will "
				+ "be divided into chunks of this length. Default is 1000.")
		@Macro
		public Integer maxSentenceLength;
		
		public W2VecBuilderConfig() {
			
			modelStage = "experiment";
			
			maxIter = 1;
			stepSize = 0.025;
			
			vectorSize = 100;
			windowSize = 5;
			
			minCount = 5;
			maxSentenceLength = 1000;
			
		}
	    
		@Override
		public Map<String, Object> getParamsAsMap() {
			
			Map<String, Object> params = new HashMap<>();

			params.put("maxIter", maxIter);
			params.put("stepSize", stepSize);

			params.put("vectorSize", vectorSize);
			params.put("windowSize", windowSize);
			
			params.put("minCount", minCount);
			params.put("maxSentenceLength", maxSentenceLength);

			return params;
		
		}
		
		public void validate() {
			super.validate();

			/* PARAMETERS */
			if (maxIter < 1)
				throw new IllegalArgumentException(String.format(
						"[%s] The maximum number of iterations must be at least 1.", this.getClass().getName()));

			if (stepSize <= 0D || stepSize > 1D)
				throw new IllegalArgumentException(
						String.format("[%s] The learning rate must be in interval (0, 1].", this.getClass().getName()));

			if (vectorSize < 1)
				throw new IllegalArgumentException(String.format(
						"[%s] The dimension of the feature vector must be at least 1.", this.getClass().getName()));

			if (windowSize < 1)
				throw new IllegalArgumentException(String.format(
						"[%s] The window size must be at least 1.", this.getClass().getName()));

			if (minCount < 1)
				throw new IllegalArgumentException(String.format(
						"[%s] The minimum number of word occurrences must be at least 1.", this.getClass().getName()));

			if (maxSentenceLength < 1)
				throw new IllegalArgumentException(String.format(
						"[%s] The sentence length (in words) must be at least 1.", this.getClass().getName()));

		}

		public void validateSchema(Schema inputSchema) {
			super.validateSchema(inputSchema);
			
			/* INPUT COLUMN */
			SchemaUtil.isArrayOfString(inputSchema, inputCol);
			
		}
		
	}
}
