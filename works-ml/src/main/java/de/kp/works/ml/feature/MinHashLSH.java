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

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.ml.feature.MinHashLSHModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import de.kp.works.core.BaseFeatureCompute;
import de.kp.works.core.BaseFeatureConfig;

import de.kp.works.ml.MLUtils;
import de.kp.works.core.ml.SparkMLManager;

@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("MinHashLSH")
@Description("A transformation stage that leverages a trained MinHash LSH model to project feature vectors onto hash value vectors.")
public class MinHashLSH extends BaseFeatureCompute {
	/*
	 * The MinHash LSH algorithm is based on binary vectors, i.e. this pipeline stage
	 * must have a vector binarization stage in front of it.
	 */
	private static final long serialVersionUID = 1435132891116854304L;

	private MinHashLSHModel model;

	public MinHashLSH(MinHashLSHConfig config) {
		this.config = config;
	}

	@Override
	public void initialize(SparkExecutionPluginContext context) throws Exception {
		((MinHashLSHConfig)config).validate();

		modelFs = SparkMLManager.getFeatureFS(context);
		modelMeta = SparkMLManager.getFeatureMeta(context);

		model = new MinHashLSHManager().read(modelFs, modelMeta, config.modelName);
		if (model == null)
			throw new IllegalArgumentException(String.format("[%s] A feature model with name '%s' does not exist.",
					this.getClass().getName(), config.modelName));

	}

	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {

		((MinHashLSHConfig)config).validate();

		StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
		/*
		 * Try to determine input and output schema; if these schemas are not explicitly
		 * specified, they will be inferred from the provided data records
		 */
		inputSchema = stageConfigurer.getInputSchema();
		if (inputSchema != null) {
			
			validateSchema(inputSchema, config);
			/*
			 * In cases where the input schema is explicitly provided, we determine the
			 * output schema by explicitly adding the output column
			 */
			outputSchema = getOutputSchema(inputSchema, config.outputCol);
			stageConfigurer.setOutputSchema(outputSchema);

		}

	}
	
	@Override
	public void validateSchema(Schema inputSchema, BaseFeatureConfig config) {
		super.validateSchema(inputSchema, config);
		
		/** INPUT COLUMN **/
		isArrayOfNumeric(config.inputCol);
		
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
		 * In v2.1.3 the MinHashLSH model does not contain
		 * methods setInputCol & setOutputCol
		 */
		model.set(model.inputCol(), config.inputCol);
		model.set(model.outputCol(), config.outputCol);
		/*
		 * The type of outputCol is Seq[Vector] where the dimension of the array
		 * equals numHashTables, and the dimensions of the vectors are currently 
		 * set to 1. 
		 * 
		 * In future releases, we will implement AND-amplification so that users 
		 * can specify the dimensions of these vectors.
		 * 
		 * For compliances purposes with CDAP data schemas, we have to resolve
		 * the output format as Array Of Double
		 */
		Dataset<Row> output = MLUtils.flattenMinHash(model.transform(source), config.outputCol);
		return output;

	}

	/**
	 * A helper method to compute the output schema in that use cases where an input
	 * schema is explicitly given
	 */
	public Schema getOutputSchema(Schema inputSchema, String outputField) {

		List<Schema.Field> fields = new ArrayList<>(inputSchema.getFields());
		
		fields.add(Schema.Field.of(outputField, Schema.arrayOf(Schema.of(Schema.Type.DOUBLE))));
		return Schema.recordOf(inputSchema.getRecordName() + ".transformed", fields);

	}	

	public static class MinHashLSHConfig extends BaseFeatureConfig {

		private static final long serialVersionUID = 8801441172298876792L;

		public void validate() {
			super.validate();

		}
	}

}
