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

import com.google.gson.Gson;
import de.kp.works.core.SchemaUtil;
import de.kp.works.core.feature.FeatureModelConfig;
import de.kp.works.core.feature.FeatureSink;
import de.kp.works.core.recording.feature.CountVecRecorder;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import io.cdap.cdap.etl.api.batch.SparkSink;
import org.apache.spark.ml.feature.CountVectorizer;
import org.apache.spark.ml.feature.CountVectorizerModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.HashMap;
import java.util.Map;

@Plugin(type = SparkSink.PLUGIN_TYPE)
@Name("CountVecBuilder")
@Description("A building stage for an Apache Spark ML Count Vectorizer model.")
public class CountVecBuilder extends FeatureSink {

	private static final long serialVersionUID = 2389361295065144103L;

	private final CountVecBuilderConfig config;
	
	public CountVecBuilder(CountVecBuilderConfig config) {
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
		
		CountVectorizer trainer = new CountVectorizer();
		trainer.setInputCol(config.inputCol);
		
		Double minDF = new Double(config.minDF);
		trainer.setMinDF(minDF);
		
		Double minTF = new Double(config.minTF);
		trainer.setMinTF(minTF);

		if (config.vocabSize != null)
			trainer.setVocabSize(config.vocabSize);
		
		CountVectorizerModel model = trainer.fit(source);		
		Map<String,Object> metrics = new HashMap<>();
		
		String modelParams = config.getParamsAsJSON();
		String modelMetrics = new Gson().toJson(metrics);
		
		String modelName = config.modelName;
		String modelStage = config.modelStage;
		
		new CountVecRecorder(configReader)
				.track(context, modelName, modelStage, modelParams, modelMetrics, model);
		
	}

	public static class CountVecBuilderConfig extends FeatureModelConfig {

		private static final long serialVersionUID = 7825023669549623718L;
		
		@Description("The maximum size of the vocabulary. If this value is smaller than the total number of different "
				+ "terms, the vocabulary will contain the top terms ordered by term frequency across the corpus.")
		@Macro
		public Integer vocabSize;

		@Description("Specifies the minimum nonnegative number of different documents a term must appear in to be included "
				+ "in the vocabulary. Default is 1.")
		@Macro
		public Integer minDF;
		
		@Description("Filter to ignore rare words in a document. For each document, terms with frequency (or count) "
				+ "less than the given threshold are ignored. Default is 1.")
		@Macro
		public Integer minTF;
		
		public CountVecBuilderConfig() {
			
			modelStage = "experiment";
			
			minDF = 1;	
			minTF = 1;
		}

		@Override
		public Map<String, Object> getParamsAsMap() {

			Map<String, Object> params = new HashMap<>();
			params.put("minDF", minDF);
			params.put("minTF", minTF);

			if (vocabSize != null)
				params.put("vocabSize", vocabSize);

			return params;

		}
		
		public void validate() {
			super.validate();

			/* PARAMETERS */
			if (vocabSize < 1)
				throw new IllegalArgumentException(String.format(
						"[%s] The maximum size of the vocabulary must be at least 1.", this.getClass().getName()));
			
			if (minDF < 1)
				throw new IllegalArgumentException(String.format(
						"[%s] The minimum of term occurences in all documents must be at least 1.", this.getClass().getName()));
			
			if (minTF < 1)
				throw new IllegalArgumentException(String.format(
						"[%s] The minimum of term occurences in each document must be at least 1.", this.getClass().getName()));

		}
		
		public void validateSchema(Schema inputSchema) {
			super.validateSchema(inputSchema);
			
			/* INPUT COLUMN */
			SchemaUtil.isArrayOfString(inputSchema, inputCol);
			
		}
		
	}
}
