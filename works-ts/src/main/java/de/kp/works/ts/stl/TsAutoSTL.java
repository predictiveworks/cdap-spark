package de.kp.works.ts.stl;
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
import java.util.stream.Stream;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.google.common.base.Strings;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import de.kp.works.core.time.TimeCompute;
import de.kp.works.ts.AutoSTL;

@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("TsAutoSTL")
@Description("A time series transformation stage to decompose each time signal into seasonality, "
		+ "trend and remainder component leveraging an STL algorithm (Seasonal and Trend decomposition using Loess). "
		+ "The periodicity required for the SL algorithm is determined automatically through an embedded ACF."
		+ "This transformation stage adds 'seasonal', 'trend' and 'remainder' fields to the each time record.")
public class TsAutoSTL extends TimeCompute {

	private static final long serialVersionUID = -4933626546193785571L;
	
	public TsAutoSTL(TsAutoSTLConfig config) {
		this.config = config;
	}
	
	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {

		((TsAutoSTLConfig)config).validate();

		StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
		/*
		 * Try to determine input and output schema; if these schemas are not explicitly
		 * specified, they will be inferred from the provided data records
		 */
		inputSchema = stageConfigurer.getInputSchema();
		if (inputSchema != null) {
			/*
			 * In cases where the input schema is explicitly provided, we determine the
			 * output schema and change the data type of the value field to DOUBLE
			 */
			outputSchema = getOutputSchema(inputSchema);
			stageConfigurer.setOutputSchema(outputSchema);

		}

	}
	@Override
	public Dataset<Row> compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {

		TsAutoSTLConfig computeConfig = (TsAutoSTLConfig)config;
		
		AutoSTL decomposer = new AutoSTL();
		
		/** COLUMNS **/
		decomposer.setTimeCol(computeConfig.timeCol);
		decomposer.setValueCol(computeConfig.valueCol);
		decomposer.setGroupCol(computeConfig.groupCol);
		
		/** PARAMETERS **/
		decomposer.setOuterIter(computeConfig.outerIter);
		decomposer.setInnerIter(computeConfig.innerIter);

		decomposer.setSeasonalLoessSize(computeConfig.seasonalLoessSize);
		decomposer.setTrendLoessSize(computeConfig.trendLoessSize);
		
		decomposer.setLevelLoessSize(computeConfig.levelLoessSize);
		
		decomposer.setThreshold(computeConfig.threshold);
		if (Strings.isNullOrEmpty(computeConfig.lagValues))
			decomposer.setMaxLag(computeConfig.maxLag);

		else
			decomposer.setLagValues(computeConfig.getLagValues());
		
		
		Dataset<Row> output = decomposer.transform(source);
		return output;
	
	}
	
	public static class TsAutoSTLConfig extends BaseSTLConfig {

		private static final long serialVersionUID = -3984501613672361196L;

		@Description("The maximum lag value. Use this parameter if the ACF is based on a range of lags. Default is 1.")
		@Macro
		public Integer maxLag;

		@Description("The comma-separated sequence of lag value. Use this parameter if the ACF should be based on discrete values. "
				+ "This sequence is empty by default.")
		@Macro
		public String lagValues;

		@Description("The threshold used to determine the lag value with the highest correlation score. Default is 0.95")
		@Macro
		public Double threshold;

		public TsAutoSTLConfig() {
			maxLag = 1;
			lagValues = "";
			threshold = 0.95;
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
