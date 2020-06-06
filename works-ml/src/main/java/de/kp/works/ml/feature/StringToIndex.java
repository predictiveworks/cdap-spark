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

import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.SparkCompute;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import de.kp.works.core.feature.FeatureConfig;
import de.kp.works.core.SchemaUtil;
import de.kp.works.core.feature.FeatureCompute;

@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("StringToIndex")
@Description("A transformation stage that leverages the Apache Spark ML StringIndexer. This stage requires a trained StringIndexer model.")
public class StringToIndex extends FeatureCompute {

	private static final long serialVersionUID = -4361931347919726410L;

	private StringToIndexConfig config;
	private StringIndexerModel model;

	public StringToIndex(StringToIndexConfig config) {
		this.config = config;
	}

	@Override
	public void initialize(SparkExecutionPluginContext context) throws Exception {
		config.validate();

		StringIndexerRecorder recorder = new StringIndexerRecorder();
		/*
		 * STEP #1: Retrieve the trained feature model that refers 
		 * to the provide name, stage and option. String Indexer models 
		 * do not have any metrics, i.e. there is no model option: 
		 * always the latest model is used
		 */
		model = recorder.read(context, config.modelName, config.modelStage, LATEST_MODEL);
		if (model == null)
			throw new IllegalArgumentException(String.format("[%s] A feature model with name '%s' does not exist.",
					this.getClass().getName(), config.modelName));

		/*
		 * STEP #2: Retrieve the profile of the trained feature 
		 * model for subsequent annotation
		 */
		profile = recorder.getProfile();

	}

	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {

		config.validate();

		StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
		/*
		 * Try to determine input and output schema; if these schemas are not explicitly
		 * specified, they will be inferred from the provided data records
		 */
		inputSchema = stageConfigurer.getInputSchema();
		if (inputSchema != null) {

			validateSchema(inputSchema);
			/*
			 * In cases where the input schema is explicitly provided, we determine the
			 * output schema by explicitly adding the output column
			 */
			outputSchema = getOutputSchema(inputSchema, config.outputCol, Schema.Type.DOUBLE);
			stageConfigurer.setOutputSchema(outputSchema);

		}

	}
	
	@Override
	public void validateSchema(Schema inputSchema) {
		config.validateSchema(inputSchema);
	}

	/**
	 * This method computes the transformed features by applying a trained
	 * StringIndexer model; as a result, the source dataset is enriched by
	 * an extra column (outputCol) that specifies the target variable in 
	 * form of a Double
	 */
	@Override
	public Dataset<Row> compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {
		
		/*
		 * Transformation from [String] to [Double]
		 */
		model.setInputCol(config.inputCol);
		model.setOutputCol(config.outputCol);

		Dataset<Row> output = model.transform(source);
		return annotate(output, FEATURE_TYPE);

	}

	public static class StringToIndexConfig extends FeatureConfig {

		private static final long serialVersionUID = -3630481748921019607L;

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
