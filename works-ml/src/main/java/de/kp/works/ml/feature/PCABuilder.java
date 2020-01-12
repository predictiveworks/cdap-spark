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

import org.apache.spark.ml.feature.PCAModel;
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
@Name("PCABuilder")
@Description("A building stage for an Apache Spark based Principal Component Analysis feature model.")
public class PCABuilder extends BaseFeatureSink {

	private static final long serialVersionUID = -698695950116408878L;

	public PCABuilder(PCABuilderConfig config) {
		this.config = config;
		this.className = PCABuilder.class.getName();
	}

	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
		super.configurePipeline(pipelineConfigurer);

		/* Validate configuration */
		((PCABuilderConfig)config).validate();

		StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();

		inputSchema = stageConfigurer.getInputSchema();
		if (inputSchema != null)
			/*
			 * Check whether the input columns is of data type
			 * Array[Double]
			 */
			validateSchema(inputSchema, config);

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
		String paramsJson = config.getParamsAsJSON();
		String metricsJson = new Gson().toJson(metrics);

		String modelName = config.modelName;
		new PCAManager().save(modelFs, modelMeta, modelName, paramsJson, metricsJson, model);

	}
	
	@Override
	public void validateSchema(Schema inputSchema, BaseFeatureModelConfig config) {
		super.validateSchema(inputSchema, config);
		
		/** INPUT COLUMN **/

		Schema.Field inputCol = inputSchema.getField(config.inputCol);
		Schema.Type inputType = inputCol.getSchema().getType();

		if (!inputType.equals(Schema.Type.ARRAY)) {
			throw new IllegalArgumentException(
					String.format("[%s] The field that defines the feature vector must be an ARRAY.", className));
		}

		Schema.Type inputCompType = inputCol.getSchema().getComponentSchema().getType();
		if (!inputCompType.equals(Schema.Type.DOUBLE)) {
			throw new IllegalArgumentException(
					String.format("[%s] The data type of the feature value must be a DOUBLE.", className));
		}
		
		
	}

	public static class PCABuilderConfig extends BaseFeatureModelConfig {

		private static final long serialVersionUID = -8154181932750250998L;
		
		@Description("The positive number of principle components.")
		@Macro
		public Integer k;

		public Map<String, Object> getParamsAsMap() {

			Map<String, Object> params = new HashMap<String, Object>();
			params.put("k", k);

			return params;

		}
		
		public void validate() {
			super.validate();

			if (k < 1) {
				throw new IllegalArgumentException(String.format("[%s] The number of principal components must be greater than 0.",
						this.getClass().getName()));
			}
			
		}
	}
}
