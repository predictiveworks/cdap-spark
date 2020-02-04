package de.kp.works.ts;
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

import org.apache.spark.ml.regression.RandomForestRegressionModel;
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
import de.kp.works.core.TimePredictorCompute;
import de.kp.works.core.TimePredictorConfig;
import de.kp.works.core.ml.RFRegressorManager;

@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("TsPredictor")
@Description("A prediction stage that leverages a trained Apache Spark based Random Forest regressor model.")
public class TsPredictor extends TimePredictorCompute {

	private static final long serialVersionUID = 3141012240338918366L;

	private TsPredictorConfig config;

	private RandomForestRegressionModel model;
	private RFRegressorManager manager;

	public TsPredictor(TsPredictorConfig config) {
		this.config = config;
	}

	@Override
	public void initialize(SparkExecutionPluginContext context) throws Exception {
		((TsPredictorConfig)config).validate();

			model = manager.read(context, config.modelName);
			if (model == null)
				throw new IllegalArgumentException(String
						.format("[%s] A regressor model with name '%s' does not exist.", this.getClass().getName(), config.modelName));

	}

	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {

		((TsPredictorConfig)config).validate();

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
			 * output schema by explicitly adding the prediction column
			 */
			outputSchema = getOutputSchema(inputSchema);
			stageConfigurer.setOutputSchema(outputSchema);

		}

	}
	
	/**
	 * A helper method to compute the output schema in that use cases where an input
	 * schema is explicitly given
	 */
	@Override
	public Schema getOutputSchema(Schema inputSchema) {
		
		List<Schema.Field> outfields = new ArrayList<>();
		for (Schema.Field field: inputSchema.getFields()) {
			
			if (field.getName().equals(config.valueCol)) {
				outfields.add(Schema.Field.of(config.valueCol, Schema.of(Schema.Type.DOUBLE)));
				
			} else
				outfields.add(field);
		}
		
		outfields.add(Schema.Field.of(config.predictionCol, Schema.of(Schema.Type.DOUBLE)));
		return Schema.recordOf(inputSchema.getRecordName() + ".predicted", outfields);

	}
	/**
	 * This method computes predictions either by applying a trained Random Forest
	 * regression model; as a result, the source dataset is enriched by an extra 
	 * column (predictionCol) that specifies the target variable in form of a Double
	 */
	@Override
	public Dataset<Row> compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {
		
		TsPredictorConfig predictorConfig = (TsPredictorConfig)config;
		/* 
		 * Retrieve time lag from model metadata to leverage
		 * the same number of features when trained the model
		 */
		Integer timeLag = (Integer)manager.getParam(context, predictorConfig.modelName, "timeLag");
		/*
		 * Vectorization of the provided dataset
		 */		
		Lagging lagging = new Lagging();
		lagging.setLag(timeLag);
		
		lagging.setFeaturesCol("features");
		lagging.setLaggingType("features");		

		Dataset<Row> vectorset = lagging.transform(source);
		/*
		 * Leverage trained Random Forest regressor model
		 */
		model.setFeaturesCol("features");
		model.setPredictionCol(predictorConfig.predictionCol);

		Dataset<Row> predictions = model.transform(vectorset);	    

		Dataset<Row> output = predictions.drop("features");
		return output;

	}

	public static class TsPredictorConfig extends TimePredictorConfig {

		private static final long serialVersionUID = 5016642365098354770L;
		
		public void validate() {
			super.validate();
		}
		
	}
}
