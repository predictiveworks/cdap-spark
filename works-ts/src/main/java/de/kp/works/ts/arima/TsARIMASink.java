package de.kp.works.ts.arima;
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
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import de.kp.works.ts.model.ARIMA;
import de.kp.works.ts.model.ARIMAModel;
import de.kp.works.ts.params.ModelParams;

@Plugin(type = "sparksink")
@Name("TsARIMASink")
@Description("A building stage for an Apache Spark based ARIMA model for time series datasets.")
public class TsARIMASink extends ARIMASink {

	private static final long serialVersionUID = 8910121582274962981L;

	private TsARIMASinkConfig config;
	
	public TsARIMASink(TsARIMASinkConfig config) {
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
		 * STEP #2: Train ARIMA Model
		 */
		ARIMA trainer = new ARIMA();
		trainer.setValueCol(config.valueCol); 
		trainer.setTimeCol(config.timeCol);
		
		trainer.setP(config.p); 
		trainer.setD(config.d); 
		trainer.setQ(config.q); 

		trainer.setRegParam(config.regParam);
		trainer.setElasticNetParam(config.elasticNetParam);
		
		trainer.setStandardization(config.toBoolean(config.standardization));
		trainer.setFitIntercept(config.toBoolean(config.fitIntercept));
		trainer.setMeanOut(config.toBoolean(config.meanOut));

		ARIMAModel model = trainer.fit(splitted[0]);
		/*
		 * STEP #3: Leverage testset to retrieve predictions
		 * and evaluate accuracy of the trained model
		 */
	    Dataset<Row> predictions = model.transform(splitted[1]);
	    String metricsJson = model.evaluate(predictions);

	    String paramsJson = config.getParamsAsJSON();
		/*
		 * STEP #3: Store trained regression model including
		 * its associated parameters and metrics
		 */		
		String modelName = config.modelName;
		new ARIMAManager().saveARIMA(modelFs, modelMeta, modelName, paramsJson, metricsJson, model);
	    
	}

	@Override
	public void validateSchema(Schema inputSchema) {
		config.validateSchema(inputSchema);
	}
	
	public static class TsARIMASinkConfig extends ARIMASinkConfig {

		private static final long serialVersionUID = -4780210026820266699L;

		@Description(ModelParams.P_PARAM_DESC)
		@Macro
		public Integer p;

		@Description(ModelParams.D_PARAM_DESC)
		@Macro
		public Integer d;

		@Description(ModelParams.Q_PARAM_DESC)
		@Macro
		public Integer q;
		
		public TsARIMASinkConfig() {

			timeSplit = "70:30";

			elasticNetParam = 0.0;
			regParam = 0.0;
			
			fitIntercept = "true";
			standardization = "true";

			meanOut = "false";
			
		}
	    
		@Override
		public Map<String, Object> getParamsAsMap() {
			
			Map<String, Object> params = new HashMap<>();			
			params.put("timeSplit", timeSplit);
			
			params.put("p", p);
			params.put("d", d);
			params.put("q", q);

			params.put("elasticNetParam", elasticNetParam);
			params.put("regParam", regParam);
			
			params.put("fitIntercept", fitIntercept);
			params.put("standardization", standardization);

			params.put("meanOut", meanOut);
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

			if (q < 1)
				throw new IllegalArgumentException(String
						.format("[%s] The order of the moving average must be positive.", this.getClass().getName()));

			
		}
		
	}

}
