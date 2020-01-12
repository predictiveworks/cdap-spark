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

import org.apache.spark.ml.feature.CountVectorizer;
import org.apache.spark.ml.feature.CountVectorizerModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.google.gson.Gson;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import de.kp.works.core.BaseFeatureModelConfig;
import de.kp.works.core.BaseFeatureSink;

@Plugin(type = "sparksink")
@Name("CountVecBuilder")
@Description("A building stage for an Apache Spark based CountVectorizer model.")
public class CountVecBuilder extends BaseFeatureSink {

	private static final long serialVersionUID = 2389361295065144103L;

	public CountVecBuilder(CountVecBuilderConfig config) {
		this.config = config;
		this.className = CountVecBuilder.class.getName();
	}

	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
		super.configurePipeline(pipelineConfigurer);

		/* Validate configuration */
		((CountVecBuilderConfig)config).validate();

		/* Validate schema */
		StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
		inputSchema = stageConfigurer.getInputSchema();
		if (inputSchema != null)
			validateSchema(inputSchema, config);

	}
	
	@Override
	public void compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {
		
		CountVecBuilderConfig vocabConfig = (CountVecBuilderConfig)config;
		
		CountVectorizer trainer = new CountVectorizer();
		trainer.setInputCol(config.inputCol);
		
		Double minDF = new Double(vocabConfig.minDF);
		trainer.setMinDF(minDF);
		
		Double minTF = new Double(vocabConfig.minTF);
		trainer.setMinTF(minTF);

		if (vocabConfig.vocabSize != null)
			trainer.setVocabSize(vocabConfig.vocabSize);
		
		CountVectorizerModel model = trainer.fit(source);		
		Map<String,Object> metrics = new HashMap<>();
		
		String paramsJson = config.getParamsAsJSON();
		String metricsJson = new Gson().toJson(metrics);
		
		String modelName = config.modelName;
		new CountVecManager().save(modelFs, modelMeta, modelName, paramsJson, metricsJson, model);
		
	}

	public static class CountVecBuilderConfig extends BaseFeatureModelConfig {

		private static final long serialVersionUID = 7825023669549623718L;
		
		@Description("The maximum size of the vocabulary. If this value is smaller than the total number of different "
				+ "terms, the vocabulary will contain the top terms ordered by term frequency across the corpus.")
		@Macro
		@Nullable
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

			/** PARAMETERS **/
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
	}
}
