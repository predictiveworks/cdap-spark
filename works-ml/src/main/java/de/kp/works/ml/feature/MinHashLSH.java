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

import de.kp.works.core.recording.feature.MinHashLSHRecorder;
import org.apache.spark.ml.feature.MinHashLSHModel;
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
import de.kp.works.core.recording.MLUtils;

@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("MinHashLSH")
@Description("A transformation stage that leverages a trained Apache Spark ML MinHash LSH model"
		+ " to project feature vectors onto hash value vectors.")
public class MinHashLSH extends FeatureCompute {
	/*
	 * The MinHash LSH algorithm is based on binary vectors, i.e. this pipeline stage
	 * must have a vector binarization stage in front of it.
	 */
	private static final long serialVersionUID = 1435132891116854304L;

	private final MinHashLSHConfig config;
	private MinHashLSHModel model;

	public MinHashLSH(MinHashLSHConfig config) {
		this.config = config;
	}

	@Override
	public void initialize(SparkExecutionPluginContext context) throws Exception {
		config.validate();

		MinHashLSHRecorder recorder = new MinHashLSHRecorder(configReader);
		/*
		 * STEP #1: Retrieve the trained feature model that refers 
		 * to the provide name, stage and option. MinimumHash LSH 
		 * models do not have any metrics, i.e. there is no model 
		 * option: always the latest model is used
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
			outputSchema = getArrayOutputSchema(inputSchema, config.outputCol, Schema.Type.DOUBLE);
			stageConfigurer.setOutputSchema(outputSchema);

		}

	}
	
	@Override
	public void validateSchema(Schema inputSchema) {
		config.validateSchema(inputSchema);
	}

	/**
	 * This method computes the transformed features by applying a trained
	 * MinHashLSH model; as a result, the source dataset is enriched by an 
	 * extra column (outputCol) that specifies the target variable in form 
	 * of an Array[Double]
	 */
	@Override
	public Dataset<Row> compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {

		/*
		 * Transformation from Array[Numeric] to Array[Double]
		 * 
		 * Build internal column from input column and cast to 
		 * double vector
		 */
		Dataset<Row> vectorset = MLUtils.vectorize(source, config.inputCol, "_input", true);
		
		/*
		 * In v2.1.3 the MinHashLSH model does not contain
		 * methods setInputCol & setOutputCol
		 */
		model.set(model.inputCol(), "_input");
		model.set(model.outputCol(), config.outputCol);
		/*
		 * The type of outputCol is Seq[Vector] where the dimension of the array
		 * equals numHashTables, and the dimensions of the vectors are currently 
		 * set to 1. 
		 * 
		 * In future releases, we will implement AND-amplification so that users 
		 * can specify the dimensions of these vectors.
		 * 
		 * For compliance purposes with CDAP data schemas, we have to resolve
		 * the output format as Array Of Double
		 */
		Dataset<Row> output = MLUtils.flattenMinHash(model.transform(vectorset), config.outputCol).drop("_input");
		return annotate(output, FEATURE_TYPE);

	}

	public static class MinHashLSHConfig extends FeatureConfig {

		private static final long serialVersionUID = 8801441172298876792L;

		public void validate() {
			super.validate();

		}
		
		public void validateSchema(Schema inputSchema) {
			super.validateSchema(inputSchema);
			
			SchemaUtil.isArrayOfNumeric(inputSchema, inputCol);
			
		}
	}

}
