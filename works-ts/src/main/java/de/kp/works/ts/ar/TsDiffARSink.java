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

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import de.kp.works.ts.model.DiffAutoRegression;
import de.kp.works.ts.model.DiffAutoRegressionModel;
import de.kp.works.ts.params.ModelParams;

@Plugin(type = "sparksink")
@Name("TsDiffARSink")
@Description("A building stage for an Apache Spark based Differencing AutoRegression model for time series datasets.")
public class TsDiffARSink extends BaseARSink {

	private static final long serialVersionUID = 539646198032768805L;
	
	public TsDiffARSink(TsDiffARSinkConfig config) {
		this.config = config;
	}

	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
		super.configurePipeline(pipelineConfigurer);

		/* Validate configuration */
		((TsDiffARSinkConfig)config).validate();
		
		/* Validate schema */
		StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
		inputSchema = stageConfigurer.getInputSchema();
		if (inputSchema != null)
			validateSchema(inputSchema, config);

	}
	
	@Override
	public void compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {

		TsDiffARSinkConfig sinkConfig = (TsDiffARSinkConfig)config;
		/*
		 * STEP #1: Split dataset into training & test timeseries
		 */
		Dataset<Row>[] splitted = sinkConfig.split(source);
		/*
		 * STEP #2: Train DiffAutoRegression Model
		 */
		DiffAutoRegression trainer = new DiffAutoRegression();
		trainer.setValueCol(sinkConfig.valueCol); 
		trainer.setTimeCol(sinkConfig.timeCol);
		
		trainer.setP(sinkConfig.p); 
		trainer.setD(sinkConfig.d); 

		trainer.setRegParam(sinkConfig.regParam);
		trainer.setElasticNetParam(sinkConfig.elasticNetParam);
		
		trainer.setStandardization(sinkConfig.toBoolean(sinkConfig.standardization));
		trainer.setFitIntercept(sinkConfig.toBoolean(sinkConfig.fitIntercept));

		DiffAutoRegressionModel model = trainer.fit(splitted[0]);
		/*
		 * STEP #3: Leverage testset to retrieve predictions
		 * and evaluate accuracy of the trained model
		 */
	    Dataset<Row> predictions = model.transform(splitted[1]);
	    String metricsJson = model.evaluate(predictions);

	    String paramsJson = sinkConfig.getParamsAsJSON();
		/*
		 * STEP #3: Store trained regression model including
		 * its associated parameters and metrics
		 */		
		String modelName = sinkConfig.modelName;
		new ARManager().saveDiffAR(modelFs, modelMeta, modelName, paramsJson, metricsJson, model);

	}

	public static class TsDiffARSinkConfig extends ARSinkConfig {

		private static final long serialVersionUID = 1752119868942406870L;

		@Description(ModelParams.P_PARAM_DESC)
		@Macro
		public Integer p;

		@Description(ModelParams.D_PARAM_DESC)
		@Macro
		public Integer d;

		public TsDiffARSinkConfig() {

			timeSplit = "70:30";

			elasticNetParam = 0.0;
			regParam = 0.0;
			
			fitIntercept = "true";
			standardization = "true";
			
		}
	    
		@Override
		public Map<String, Object> getParamsAsMap() {
			
			Map<String, Object> params = new HashMap<>();
			
			params.put("timeSplit", timeSplit);
			params.put("p", p);
			params.put("d", d);

			params.put("elasticNetParam", elasticNetParam);
			params.put("regParam", regParam);
			
			params.put("fitIntercept", fitIntercept);
			params.put("standardization", standardization);

			return params;
		
		}

		public void validate() {
			super.validate();

			if (p < 1)
				throw new IllegalArgumentException(String
						.format("[%s] The number of lag observations must be positive.", this.getClass().getName()));
			
			if (d < 1)
				throw new IllegalArgumentException(String
						.format("[%s] The degree of differencing must be positive.", this.getClass().getName()));
			
		}
		
	}

}
