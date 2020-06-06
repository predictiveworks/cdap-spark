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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.SparkCompute;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import de.kp.works.core.regressor.RFRRecorder;
import de.kp.works.core.time.TimeCompute;
import de.kp.works.core.time.TimePredictorConfig;

@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("TsPredictor")
@Description("A prediction stage that leverages a trained Apache Spark ML Random Forest regressor model.")
public class TsPredictor extends TimeCompute {

	private static final long serialVersionUID = 3141012240338918366L;

	private TsPredictorConfig config;
	private RFRRecorder recorder;
	
	private RandomForestRegressionModel regressor;

	public TsPredictor(TsPredictorConfig config) {
		this.config = config;
	}

	@Override
	public void initialize(SparkExecutionPluginContext context) throws Exception {
		config.validate();

			recorder = new RFRRecorder();
			/* 
			 * STEP #1: Retrieve the trained regression model
			 * that refers to the provide name, stage and option
			 */
			regressor = recorder.read(context, config.modelName, config.modelStage, config.modelOption);
			if (regressor == null)
				throw new IllegalArgumentException(String
						.format("[%s] A regressor model with name '%s' does not exist.", this.getClass().getName(), config.modelName));

			/* 
			 * STEP #2: Retrieve the profile of the trained
			 * regression model for subsequent annotation
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
			 * output schema by explicitly adding the prediction column
			 */
			outputSchema = getOutputSchema(inputSchema);
			stageConfigurer.setOutputSchema(outputSchema);

		}

	}
	/**
	 * This method computes predictions either by applying a trained Random Forest
	 * regression model; as a result, the source dataset is enriched by an extra 
	 * column (predictionCol) that specifies the target variable in form of a Double
	 */
	@Override
	public Dataset<Row> compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {
		
		/* 
		 * Retrieve time lag from model metadata to leverage
		 * the same number of features when trained the model
		 */
		Integer timeLag = (Integer)recorder.getParam(context, config.modelName, "timeLag");
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
		regressor.setFeaturesCol("features");
		regressor.setPredictionCol(config.predictionCol);

		Dataset<Row> predictions = regressor.transform(vectorset);	    
		/*
		 * Remove intermediate features column from predictions
		 * and annotate each prediction with the model profile
		 */
		Dataset<Row> output = predictions.drop("features");
		return annotate(output, REGRESSOR_TYPE);

	}
	
	/**
	 * A helper method to compute the output schema in that use cases where an input
	 * schema is explicitly given
	 */
	public Schema getOutputSchema(Schema inputSchema) {
		
		List<Schema.Field> outfields = new ArrayList<>();
		for (Schema.Field field: inputSchema.getFields()) {
			
			if (field.getName().equals(config.valueCol)) {
				outfields.add(Schema.Field.of(config.valueCol, Schema.of(Schema.Type.DOUBLE)));
				
			} else
				outfields.add(field);
		}

		outfields.add(Schema.Field.of(config.predictionCol, Schema.of(Schema.Type.DOUBLE)));
		/* 
		 * Check whether the input schema already has an 
		 * annotation field defined; the predictor stage
		 * may not be the first stage that annotates model
		 * specific metadata 
		 */
		if (inputSchema.getField(ANNOTATION_COL) == null)
			outfields.add(Schema.Field.of(ANNOTATION_COL, Schema.of(Schema.Type.STRING)));
		
		return Schema.recordOf(inputSchema.getRecordName() + ".predicted", outfields);

	}

	@Override
	public void validateSchema(Schema inputSchema) {
		config.validateSchema(inputSchema);
	}

	public static class TsPredictorConfig extends TimePredictorConfig {

		private static final long serialVersionUID = 5016642365098354770L;
		
		public void validate() {
			super.validate();
		}
		
	}
}
