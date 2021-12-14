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

import de.kp.works.core.recording.feature.PCARecorder;
import org.apache.spark.ml.feature.PCAModel;
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
@Name("PCABuilder")
@Description("A building stage for an Apache Spark based Principal Component Analysis feature model.")
public class PCABuilder extends FeatureSink {

	private static final long serialVersionUID = -698695950116408878L;

	private final PCABuilderConfig config;
	
	public PCABuilder(PCABuilderConfig config) {
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
			 * Array[Double]
			 */
			validateSchema(inputSchema);

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
		PCATrainer trainer = new PCATrainer();
		Dataset<Row> vectorset = trainer.vectorize(source, featuresCol, vectorCol);

		PCAModel model = trainer.train(vectorset, vectorCol, params);

		Map<String, Object> metrics = new HashMap<>();
		/*
		 * Store trained PCA model including its associated parameters and
		 * metrics
		 */
		String modelParams = config.getParamsAsJSON();
		String modelMetrics = new Gson().toJson(metrics);

		String modelName = config.modelName;
		String modelStage = config.modelStage;
		
		new PCARecorder(configReader)
				.track(context, modelName, modelStage, modelParams, modelMetrics, model);

	}
	
	@Override
	public void validateSchema(Schema inputSchema) {
		config.validateSchema(inputSchema);
	}

	public static class PCABuilderConfig extends FeatureModelConfig {

		private static final long serialVersionUID = -8154181932750250998L;
		
		@Description("The positive number of principle components.")
		@Macro
		public Integer numComp;

		public PCABuilderConfig() {
			modelStage = "experiment";
		}
		
		public Map<String, Object> getParamsAsMap() {

			Map<String, Object> params = new HashMap<>();
			params.put("numComp", numComp);

			return params;

		}
		
		public void validate() {
			super.validate();

			if (numComp < 1) {
				throw new IllegalArgumentException(String.format("[%s] The number of principal components must be greater than 0.",
						this.getClass().getName()));
			}
			
		}
		
		public void validateSchema(Schema inputSchema) {
			super.validateSchema(inputSchema);
			
			SchemaUtil.isArrayOfNumeric(inputSchema, inputCol);
			
		}
	}
}
