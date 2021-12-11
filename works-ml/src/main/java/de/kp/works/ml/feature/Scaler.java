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

import de.kp.works.core.recording.feature.ScalerRecorder;
import org.apache.spark.ml.feature.MaxAbsScalerModel;
import org.apache.spark.ml.feature.MinMaxScalerModel;
import org.apache.spark.ml.feature.StandardScalerModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
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
@Name("Scaler")
@Description("A transformation stage that leverages a trained Scaler model to project feature vectors onto scaled vectors. "
		+ "Supported models are 'Max-Abs', 'Min-Max' and 'Standard'.")
public class Scaler extends FeatureCompute {

	private static final long serialVersionUID = -2419787880853896958L;

	private ScalerConfig config;
	
	private MaxAbsScalerModel maxAbsModel;
	private MinMaxScalerModel minMaxModel;
	private StandardScalerModel standardModel;

	public Scaler(ScalerConfig config) {
		this.config = config;
	}

	@Override
	public void initialize(SparkExecutionPluginContext context) throws Exception {

		config.validate();

		ScalerRecorder recorder = new ScalerRecorder();
		String modelType = config.modelType;
		/*
		 * STEP #1: Retrieve the trained feature model that refers 
		 * to the provide name, stage and option. Scaler models do 
		 * not have any metrics, i.e. there is no model option: 
		 * always the latest model is used
		 */
		if (modelType.equals("maxabs")) {

			maxAbsModel = recorder.readMaxAbsScaler(context, config.modelName, config.modelStage, LATEST_MODEL);
			if (maxAbsModel == null)
				throw new IllegalArgumentException(String.format("[%s] A feature model with name '%s' does not exist.",
						this.getClass().getName(), config.modelName));
		
		} else if (modelType.equals("minmax")) {

			minMaxModel = recorder.readMinMaxScaler(context, config.modelName, config.modelStage, LATEST_MODEL);
			if (minMaxModel == null)
				throw new IllegalArgumentException(String.format("[%s] A feature model with name '%s' does not exist.",
						this.getClass().getName(), config.modelName));
					
		} else {

			standardModel = recorder.readStandardScaler(context, config.modelName, config.modelStage, LATEST_MODEL);
			if (standardModel == null)
				throw new IllegalArgumentException(String.format("[%s] A feature model with name '%s' does not exist.",
						this.getClass().getName(), config.modelName));
					
		}

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

	@Override
	public Dataset<Row> compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {
		
		/*
		 * Transformation from Array[Numeric] to Array[Double]
		 *
		 * Build internal column from input column and cast to 
		 * double vector
		 */
		Dataset<Row> vectorset = MLUtils.vectorize(source, config.inputCol, "_input", true);
		Dataset<Row> transformed;

		String modelType = config.modelType;
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
		return annotate(output, FEATURE_TYPE);
		
	}

	public static class ScalerConfig extends FeatureConfig {

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
		
		public void validateSchema(Schema inputSchema) {
			super.validateSchema(inputSchema);
			
			SchemaUtil.isArrayOfNumeric(inputSchema, inputCol);
			
		}
		
	}

}
