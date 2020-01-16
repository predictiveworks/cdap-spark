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

import org.apache.spark.ml.feature.MaxAbsScalerModel;
import org.apache.spark.ml.feature.MinMaxScalerModel;
import org.apache.spark.ml.feature.StandardScalerModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import de.kp.works.core.BaseFeatureCompute;
import de.kp.works.core.BaseFeatureConfig;
import de.kp.works.core.ml.SparkMLManager;
import de.kp.works.ml.MLUtils;

@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("Scaler")
@Description("A transformation stage that leverages a trained Scaler model to project feature vectors onto scaled vectors. "
		+ "Supported models are 'Max-Abs', 'Min-Max' and 'Standard'.")
public class Scaler extends BaseFeatureCompute {

	private static final long serialVersionUID = -2419787880853896958L;

	private MaxAbsScalerModel maxAbsModel;
	private MinMaxScalerModel minMaxModel;
	private StandardScalerModel standardModel;

	public Scaler(ScalerConfig config) {
		this.config = config;
	}

	@Override
	public void initialize(SparkExecutionPluginContext context) throws Exception {

		ScalerConfig scalerConfig = (ScalerConfig) config;
		scalerConfig.validate();

		modelFs = SparkMLManager.getFeatureFS(context);
		modelMeta = SparkMLManager.getFeatureMeta(context);

		ScalerManager manager = new ScalerManager();
		String modelType = scalerConfig.modelType;

		if (modelType.equals("maxabs")) {

			maxAbsModel = manager.readMaxAbsScaler(modelFs, modelMeta, config.modelName);
			if (maxAbsModel == null)
				throw new IllegalArgumentException(String.format("[%s] A feature model with name '%s' does not exist.",
						this.getClass().getName(), config.modelName));
		
		} else if (modelType.equals("minmax")) {

			minMaxModel = manager.readMinMaxScaler(modelFs, modelMeta, config.modelName);
			if (minMaxModel == null)
				throw new IllegalArgumentException(String.format("[%s] A feature model with name '%s' does not exist.",
						this.getClass().getName(), config.modelName));
					
		} else {

			standardModel = manager.readStandardScaler(modelFs, modelMeta, config.modelName);
			if (standardModel == null)
				throw new IllegalArgumentException(String.format("[%s] A feature model with name '%s' does not exist.",
						this.getClass().getName(), config.modelName));
					
		}

	}

	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {

		((ScalerConfig)config).validate();

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
	 * A helper method to compute the output schema in that use cases where an input
	 * schema is explicitly given
	 */
	public Schema getOutputSchema(Schema inputSchema, String outputField) {

		List<Schema.Field> fields = new ArrayList<>(inputSchema.getFields());

		fields.add(Schema.Field.of(outputField, Schema.arrayOf(Schema.of(Schema.Type.DOUBLE))));
		return Schema.recordOf(inputSchema.getRecordName() + ".transformed", fields);

	}
	@Override
	public Dataset<Row> compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {
		
		/*
		 * Transformation from Array[Numeric] to Array[Double]
		 */
		ScalerConfig scalerConfig = (ScalerConfig)config;
		/*
		 * Build internal column from input column and cast to 
		 * double vector
		 */
		Dataset<Row> vectorset = MLUtils.vectorize(source, scalerConfig.inputCol, "_input", true);
		Dataset<Row> transformed;

		String modelType = scalerConfig.modelType;
		if (modelType.equals("maxabs")) {
			/*
			 * The internal output of the MaxAbs model is an ML specific
			 * vector representation; this must be transformed into
			 * an Array[Double] to be compliant with Google CDAP
			 */		
			maxAbsModel.setOutputCol("_vector");
			transformed = maxAbsModel.transform(vectorset);
			 
		} else if (modelType.equals("minmax")) {
			/*
			 * The internal output of the MinMax model is an ML specific
			 * vector representation; this must be transformed into
			 * an Array[Double] to be compliant with Google CDAP
			 */		
			minMaxModel.setOutputCol("_vector");
			transformed = minMaxModel.transform(vectorset);
			
		} else {
			/*
			 * The internal output of the Standard model is an ML specific
			 * vector representation; this must be transformed into
			 * an Array[Double] to be compliant with Google CDAP
			 */		
			standardModel.setOutputCol("_vector");
			transformed = standardModel.transform(vectorset);
			
		}

		Dataset<Row> output = MLUtils.devectorize(transformed, "_vector", config.outputCol).drop("_input").drop("_vector");
		return output;
		
	}

	public static class ScalerConfig extends BaseFeatureConfig {

		private static final long serialVersionUID = -5718960366269208424L;

		@Description("The type of the scaler model. Supported values are 'maxabs', 'minmax' and 'standard'. Default is 'standard'.")
		@Macro
		public String modelType;

		public ScalerConfig() {
			modelType = "standard";

		}

		public void validate() {
			super.validate();

		}
	}

}
