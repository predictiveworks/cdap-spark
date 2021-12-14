package de.kp.works.text.embeddings;
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

import de.kp.works.text.recording.Word2VecRecorder;
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
import de.kp.works.core.text.TextSink;

@Plugin(type = SparkSink.PLUGIN_TYPE)
@Name("Word2VecBuilder")
@Description("A building stage for an Apache Spark-NLP based Word2Vec embedding model.")
public class Word2VecBuilder extends TextSink {

	private static final long serialVersionUID = 393252026613980477L;

	private final Word2VecSinkConfig config;
	
	public Word2VecBuilder(Word2VecSinkConfig config) {
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
	public void compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {

		Map<String, Object> params = config.getParamsAsMap();
		String modelParams = config.getParamsAsJSON();
		
		Word2VecTrainer trainer = new Word2VecTrainer();
		Word2VecModel model = trainer.train(source, config.textCol, params);

		Map<String,Object> metrics = new HashMap<>();
		String modelMetrics = new Gson().toJson(metrics);

		String modelName = config.modelName;
		String modelStage = config.modelStage;
		
		new Word2VecRecorder(configReader)
				.track(context, modelName, modelStage, modelParams, modelMetrics, model);
	    
	}

	@Override
	public void validateSchema(Schema inputSchema) {

		/* TEXT COLUMN */

		Schema.Field textCol = inputSchema.getField(config.textCol);
		if (textCol == null) {
			throw new IllegalArgumentException(
					String.format("[%s] The input schema must contain the field that contains the text document.",
							this.getClass().getName()));
		}

		SchemaUtil.isString(inputSchema, config.textCol);

	}
	
	public static class Word2VecSinkConfig extends BaseWord2VecConfig {

		private static final long serialVersionUID = 6821152390553851034L;

		@Description("The maximum number of iterations to train the Word2Vec model. Default is 1.")
		@Macro
		public Integer maxIter;
		
		@Description("The learning rate for shrinking the contribution of each estimator. Must be in interval (0, 1]. Default is 0.025")
		@Macro
		public Double stepSize;

		@Description("The positive dimension of the feature vector to represent a certain word. Default is 100.")
		@Macro
		public Integer vectorSize;

		@Description("The positive window size. Default is 5.")
		@Macro
		public Integer windowSize;

		@Description("The minimum number of times a word must appear to be included in the Word2Vec vocabulary. Default is 5.")
		@Macro
		public Integer minCount;

		@Description("The maximum length (in words) of each sentence in the input data. Any sentence longer than this threshold will "
				+ "be divided into chunks of this length. Default is 1000.")
		@Macro
		public Integer maxSentenceLength;
	    
		public Word2VecSinkConfig() {
			
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

			params.put("normalization", getNormalization());
			return params;
		
		}
		
		public void validate() {
			super.validate();

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
		
	}
}
