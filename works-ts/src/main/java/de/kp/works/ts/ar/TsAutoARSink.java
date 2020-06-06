package de.kp.works.ts.ar;
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

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import io.cdap.cdap.etl.api.batch.SparkSink;

import de.kp.works.ts.model.AutoAR;
import de.kp.works.ts.model.AutoARModel;
import de.kp.works.ts.params.ModelParams;

@Plugin(type = SparkSink.PLUGIN_TYPE)
@Name("TsAutoARSink")
@Description("A building stage for an Apache Spark based AutoAR model for time series datasets.")
public class TsAutoARSink extends ARSink {

	private static final long serialVersionUID = -2595856794547551651L;
	
	private TsAutoARSinkConfig config;
	
	public TsAutoARSink(TsAutoARSinkConfig config) {
		this.config = config;
	}

	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
		super.configurePipeline(pipelineConfigurer);

		/* Validate configuration */
		config.validate();
		
		/* Validate schema */
		StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
		inputSchema = stageConfigurer.getInputSchema();
		if (inputSchema != null)
			validateSchema(inputSchema);

	}
	
	@Override
	public void compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {

		/*
		 * STEP #1: Split dataset into training & test timeseries
		 */
		Dataset<Row>[] splitted = config.split(source);
		/*
		 * STEP #2: Train AutoAR Model
		 */
		AutoAR trainer = new AutoAR();
		trainer.setValueCol(config.valueCol); 
		trainer.setTimeCol(config.timeCol);
		
		trainer.setPMax(config.pmax); 

		trainer.setRegParam(config.regParam);
		trainer.setElasticNetParam(config.elasticNetParam);		
		
		trainer.setStandardization(config.toBoolean(config.standardization));
		trainer.setFitIntercept(config.toBoolean(config.fitIntercept));

		trainer.setMeanOut(config.toBoolean(config.meanOut));
		trainer.setCriterion(config.criterion);

		AutoARModel model = trainer.fit(splitted[0]);
		/*
		 * STEP #3: Leverage testset to retrieve predictions
		 * and evaluate accuracy of the trained model
		 */
	    Dataset<Row> predictions = model.transform(splitted[1]);
	    String modelMetrics = model.evaluate(predictions);

	    String modelParams = config.getParamsAsJSON();
		/*
		 * STEP #3: Store trained regression model including
		 * its associated parameters and metrics
		 */		
		String modelName = config.modelName;
		String modelStage = config.modelStage;
		
		new ARRecorder().trackAutoAR(context, modelName, modelStage, modelParams, modelMetrics, model);

	}

	@Override
	public void validateSchema(Schema inputSchema) {
		config.validateSchema(inputSchema);
	}

	public static class TsAutoARSinkConfig extends ARSinkConfig {

		private static final long serialVersionUID = -5935359961422461769L;

		@Description(ModelParams.MEAN_OUT_DESC)
		@Macro
		public String meanOut;

		@Description(ModelParams.P_MAX_PARAM_DESC)
		@Macro
		public Integer pmax;

		@Description(ModelParams.CRITERION_PARAM_DESC)
		@Macro
		public String criterion;

		public TsAutoARSinkConfig() {

			timeSplit = "70:30";
			modelStage = "experiment";

			elasticNetParam = 0.0;
			regParam = 0.0;

			criterion = "aic";
			meanOut = "false";			
			
			fitIntercept = "true";
			standardization = "true";
			
		}
	    
		@Override
		public Map<String, Object> getParamsAsMap() {
			
			Map<String, Object> params = new HashMap<>();
			
			params.put("timeSplit", timeSplit);
			params.put("pmax", pmax);

			params.put("elasticNetParam", elasticNetParam);
			params.put("regParam", regParam);
			
			params.put("fitIntercept", fitIntercept);
			params.put("standardization", standardization);

			params.put("meanOut", meanOut);
			params.put("criterion", criterion);

			return params;
		
		}

		public void validate() {
			super.validate();

			if (pmax < 1)
				throw new IllegalArgumentException(String
						.format("[%s] The upper limit of the number of lag observations must be positive.", this.getClass().getName()));
			
		}
		
	}

}
