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
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import de.kp.works.ts.TimeSplit;
import de.kp.works.ts.model.ARYuleWalker;
import de.kp.works.ts.model.ARYuleWalkerModel;
import de.kp.works.ts.params.ModelParams;

public class TsYuleWalkerSink extends BaseARSink {

	private static final long serialVersionUID = 2998150184535380725L;
	
	public TsYuleWalkerSink(TsYuleWalkerSinkConfig config) {
		this.config = config;
	}

	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
		super.configurePipeline(pipelineConfigurer);

		/* Validate configuration */
		((TsYuleWalkerSinkConfig)config).validate();
		
		/* Validate schema */
		StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
		inputSchema = stageConfigurer.getInputSchema();
		if (inputSchema != null)
			validateSchema(inputSchema, config);

	}
	
	@Override
	public void compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {

		TsYuleWalkerSinkConfig sinkConfig = (TsYuleWalkerSinkConfig)config;
		/*
		 * STEP #1: Split dataset into training & test timeseries
		 */
		Dataset<Row>[] splitted = sinkConfig.split(source);
		/*
		 * STEP #2: Train ARYuleWalker Model
		 */
		ARYuleWalker trainer = new ARYuleWalker();
		trainer.setValueCol(sinkConfig.valueCol); 
		trainer.setTimeCol(sinkConfig.timeCol);
		
		trainer.setP(sinkConfig.p); 
		
		ARYuleWalkerModel model = trainer.fit(splitted[0]);
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
		new ARManager().saveYuleWalker(modelFs, modelMeta, modelName, paramsJson, metricsJson, model);

	}

	public static class TsYuleWalkerSinkConfig extends ARConfig {

		private static final long serialVersionUID = 8806382396675615715L;

		@Description(ModelParams.P_PARAM_DESC)
		@Macro
		public Integer p;

		@Description("The split of the dataset into train & test data, e.g. 80:20. Note, this is a split time "
				+ "and is computed from the total time span (min, max) of the time series. Default is 70:30")
		@Macro
		public String timeSplit;
	    
		@Override
		public Map<String, Object> getParamsAsMap() {
			
			Map<String, Object> params = new HashMap<>();

			params.put("timeSplit", timeSplit);
			params.put("p", p);

			return params;
		
		}

		public void validate() {
			super.validate();

			if (p < 1)
				throw new IllegalArgumentException(String
						.format("[%s] The number of lag observations must be positive.", this.getClass().getName()));
			
		}
		
		public Dataset<Row>[] split(Dataset<Row> source) {
			/*
			 * STEP #1: Split dataset into training & test timeseries
			 */
			TimeSplit splitter = new TimeSplit();
			splitter.setTimeCol(timeCol);
			splitter.setTimeSplit(timeSplit);
		
			Dataset<Row>[] splitted = splitter.timeSplit(source);
			return splitted;
		
		}
		
	}

}
