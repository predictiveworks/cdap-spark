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

import io.cdap.cdap.api.annotation.Description;
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
@Name("StringIndexerBuilder")
@Description("A building stage for an Apache Spark ML StringIndexer model.")
public class StringIndexerBuilder extends FeatureSink {
	/*
	 * A label indexer that maps a string column of labels to an ML column of label indices.
	 * If the input column is numeric, we cast it to string and index the string values.
	 * The indices are in [0, numLabels), ordered by label frequencies.
	 * So the most frequent label gets index 0.
	 */
	private static final long serialVersionUID = -2360022873735403321L;

	private StringIndexerBuilderConfig config;
	
	public StringIndexerBuilder(StringIndexerBuilderConfig config) {
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

		org.apache.spark.ml.feature.StringIndexer trainer = new org.apache.spark.ml.feature.StringIndexer();
		trainer.setInputCol(config.inputCol);

		StringIndexerModel model = trainer.fit(source);
		
		Map<String, Object> metrics = new HashMap<>();
		/*
		 * Store trained StringIndexer model including its associated
		 * parameters and metrics
		 */
		String modelParams = config.getParamsAsJSON();
		String modelMetrics = new Gson().toJson(metrics);

		String modelName = config.modelName;
		String modelStage = config.modelStage;
		
		new StringIndexerRecorder().track(context, modelName, modelStage, modelParams, modelMetrics, model);
		
	}
	
	@Override
	public void validateSchema(Schema inputSchema) {
		config.validateSchema(inputSchema);
	}

	public static class StringIndexerBuilderConfig extends FeatureModelConfig {

		private static final long serialVersionUID = -97589053635760766L;
		
		public StringIndexerBuilderConfig() {
			modelStage = "experiment";
		}
		
		public void validate() {
			super.validate();
		}

		public void validateSchema(Schema inputSchema) {
			super.validateSchema(inputSchema);
			
			/** INPUT COLUMN **/
			SchemaUtil.isString(inputSchema, inputCol);
			
		}
		
	}
}
