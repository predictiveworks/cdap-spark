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

import org.apache.spark.ml.feature.IDFModel;
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
import de.kp.works.core.BaseFeatureModelConfig;
import de.kp.works.core.BaseFeatureSink;

@Plugin(type = "sparksink")
@Name("TFIDFBuilder")
@Description("A building stage for an Apache Spark based TF-IDF feature model.")
public class TFIDFBuilder extends BaseFeatureSink {
	/*
	 * This model builder trains a text model that is used to transform
	 * a sequence of words (sentence) into its feature vector.
	 */
	private static final long serialVersionUID = -513344006567533602L;

	public TFIDFBuilder(TFIDFBuilderConfig config) {
		this.config = config;
		this.className = TFIDFBuilder.class.getName();
	}

	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
		super.configurePipeline(pipelineConfigurer);

		/* Validate configuration */
		((TFIDFBuilderConfig)config).validate();

		StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();

		inputSchema = stageConfigurer.getInputSchema();
		if (inputSchema != null)
			/*
			 * Check whether the input columns is of data type
			 * Array[String]
			 */
			validateSchema(inputSchema, config);

	}
	
	@Override
	public void validateSchema(Schema inputSchema, BaseFeatureModelConfig config) {
		super.validateSchema(inputSchema, config);
		
		/** INPUT COLUMN **/
		isArrayOfString(config.inputCol);
		
	}

	@Override
	public void compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {

		TFIDFBuilderConfig builderConfig = (TFIDFBuilderConfig)config;		
		Map<String, Object> params = builderConfig.getParamsAsMap();

		TFIDFTrainer trainer = new TFIDFTrainer();
		IDFModel model = trainer.train(source, builderConfig.inputCol, params);

		Map<String, Object> metrics = new HashMap<>();
		/*
		 * Store trained Word2Vec model including its associated 
		 * parameters and metrics
		 */
		String paramsJson = builderConfig.getParamsAsJSON();
		String metricsJson = new Gson().toJson(metrics);

		String modelName = builderConfig.modelName;
		new TFIDFManager().save(modelFs, modelMeta, modelName, paramsJson, metricsJson, model);

	}

	public static class TFIDFBuilderConfig extends BaseFeatureModelConfig {

		private static final long serialVersionUID = 6119701336170807824L;

		@Description("The nonnegative number of features to transform a sequence of terms into.")
		@Macro
		public Integer numFeatures;
		
		@Description("The minimum number of documents in which a term should appear. Default is 0.")
		@Macro
		public Integer minDocFreq;
		
		public TFIDFBuilderConfig() {
			minDocFreq = 0;
		}
	    
		@Override
		public Map<String, Object> getParamsAsMap() {
			
			Map<String, Object> params = new HashMap<>();

			params.put("numFeatures", numFeatures);
			params.put("minDocFreq", minDocFreq);

			return params;
		
		}

		public void validate() {
			super.validate();
			
			if (numFeatures <= 0) {
				throw new IllegalArgumentException(String
						.format("[%s] The number of features must be greater than 0.", this.getClass().getName()));
			}
			
			if (numFeatures < 0) {
				throw new IllegalArgumentException(String
						.format("[%s] The minimum number of documents must be nonnegative.", this.getClass().getName()));
			}

		}
		
	}
}
