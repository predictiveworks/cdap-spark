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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.google.common.base.Strings;
import com.google.gson.Gson;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.SparkCompute;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import de.kp.works.core.time.TimeCompute;
import de.kp.works.core.time.TimeConfig;
import de.kp.works.ts.util.ACFRecorder;

@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("TsAutoCorrelate")
@Description("A time series computation stage that determines the ACF (Auto Correlation Function) of a time series. "
		+ "This stage does not transform the input dataset, but computes and persists its ACF for ease of use in subsequent stages.")
public class TsACF extends TimeCompute {

	private static final long serialVersionUID = -2782650546004837104L;

	private TsAutoCorrelateConfig config;

	public TsACF(TsAutoCorrelateConfig config) {
		this.config = config;

	}

	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {

		StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();

		inputSchema = stageConfigurer.getInputSchema();
		if (inputSchema != null) {
			stageConfigurer.setOutputSchema(inputSchema);
		}

	}

	@Override
	public Dataset<Row> compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {

		TsAutoCorrelateConfig computeConfig = (TsAutoCorrelateConfig) config;

		AutoCorrelation computer = new AutoCorrelation();
		computer.setValueCol(computeConfig.valueCol);

		computer.setThreshold(computeConfig.threshold);
		if (Strings.isNullOrEmpty(computeConfig.lagValues))
			computer.setMaxLag(computeConfig.maxLag);

		else
			computer.setLagValues(computeConfig.getLagValues());

		AutoCorrelationModel model = computer.fit(source);

		Map<String, Object> metrics = new HashMap<>();
		String modelMetrics = new Gson().toJson(metrics);

		String modelParams = computeConfig.getParamsAsJSON();
		String modelName = computeConfig.modelName;

		ACFRecorder manager = new ACFRecorder();
		
		String modelStage = config.modelStage;
		manager.track(context, modelName, modelStage, modelParams, modelMetrics, model);

		return source;

	}

	public static class TsAutoCorrelateConfig extends TimeConfig {

		private static final long serialVersionUID = 3768876963479002781L;

		@Description("The unique name of the ACF dataset.")
		@Macro
		public String modelName;

		@Description("The stage of the ACF dataset. Supported values are 'experiment', 'stagging', 'production' and 'archived'. Default is 'experiment'.")
		@Macro
		public String modelStage;

		@Description("The maximum lag value. Use this parameter if the ACF is based on a range of lags. Default is 1.")
		@Macro
		public Integer maxLag;

		@Description("The comma-separated sequence of lag value. Use this parameter if the ACF should be based on discrete values. "
				+ "This sequence is empty by default.")
		@Macro
		public String lagValues;

		@Description("The threshold used to determine the lag value with the highest correlation score. Default is 0.95.")
		@Macro
		public Double threshold;

		public TsAutoCorrelateConfig() {
			
			modelStage = "experiment";
			
			maxLag = 1;
			lagValues = "";
			threshold = 0.95;
		}

		@Override
		public Map<String, Object> getParamsAsMap() {

			Map<String, Object> params = new HashMap<>();

			params.put("maxLag", maxLag);
			params.put("lagValues", lagValues);

			params.put("threshold", threshold);
			return params;

		}

		public int[] getLagValues() {

			String[] tokens = lagValues.split(",");

			List<Integer> lags = new ArrayList<>();
			for (String token : tokens) {
				lags.add(Integer.parseInt(token.trim()));
			}

			Integer[] array = lags.toArray(new Integer[lags.size()]);
			return Stream.of(array).mapToInt(Integer::intValue).toArray();

		}

		public void validate() {
			super.validate();

			if (Strings.isNullOrEmpty(modelName)) {
				throw new IllegalArgumentException(
						String.format("[%s] The model name must not be empty.", this.getClass().getName()));
			}

			if (maxLag == null && Strings.isNullOrEmpty(lagValues)) {
				throw new IllegalArgumentException(String.format(
						"[%s] The auto correlation function is based on provided lag values, but no values found.",
						this.getClass().getName()));
			}

			if (maxLag != null && maxLag < 1) {
				throw new IllegalArgumentException(String.format(
						"[%s] The maximum lag values must be positive, if provided.", this.getClass().getName()));
			}

			if (threshold <= 0D) {
				throw new IllegalArgumentException(
						String.format("[%s] The threshold to determine the maximum correlation score must be positive.",
								this.getClass().getName()));
			}

		}

	}
}
