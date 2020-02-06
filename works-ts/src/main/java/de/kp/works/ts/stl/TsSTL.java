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

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.google.common.base.Strings;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import de.kp.works.ts.STL;

@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("TsSTL")
@Description("A time series transformation stage to decompose each time signal into seasonality, "
		+ "trend and remainder component leveraging an STL algorithm (Seasonal and Trend decomposition using Loess). "
		+ "This transformation stage adds 'seasonal', 'trend' and 'remainder' fields to the each time record.")
public class TsSTL extends STLCompute {

	private static final long serialVersionUID = -8650664753408204785L;
	/*
	 * STL is a filtering procedure for decomposing a time series into trend, seasonal and 
	 * remainder components and consists of a sequence of applications of the LOESS smoother.
	 * 
	 */
	private TsSTLConfig config;
	
	public TsSTL(TsSTLConfig config) {
		this.config = config;
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
			 * output schema and change the data type of the value field to DOUBLE
			 */
			outputSchema = getOutputSchema(inputSchema, config.valueCol);
			stageConfigurer.setOutputSchema(outputSchema);

		}

	}
	@Override
	public Dataset<Row> compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {
		
		STL decomposer = new STL();
		
		/** COLUMNS **/
		decomposer.setTimeCol(config.timeCol);
		decomposer.setValueCol(config.valueCol);
		decomposer.setGroupCol(config.groupCol);
		
		/** PARAMETERS **/
		decomposer.setOuterIter(config.outerIter);
		decomposer.setInnerIter(config.innerIter);
		
		decomposer.setPeriodicity(config.periodicity);

		decomposer.setSeasonalLoessSize(config.seasonalLoessSize);
		decomposer.setTrendLoessSize(config.trendLoessSize);
		
		decomposer.setLevelLoessSize(config.levelLoessSize);
		
		Dataset<Row> output = decomposer.transform(source);
		return output;
	
	}

	@Override
	public void validateSchema(Schema inputSchema) {
		config.validateSchema(inputSchema);
	}
	
	public static class TsSTLConfig extends BaseSTLConfig {

		private static final long serialVersionUID = 7484792664574626489L;
		
		@Description("The periodicity of the seasonality; should be equal to lag of the autocorrelation function with the "
				+ "highest (positive) correlation.")
		@Macro
		public Integer periodicity;
		
		public void validate() {
			super.validate();

			if (Strings.isNullOrEmpty(groupCol))
				throw new IllegalArgumentException(
						String.format("[%s] The name of the field that is used for grouping must not be empty.", this.getClass().getName()));
			
			if (periodicity < 1)
				throw new IllegalArgumentException(String.format(
						"[%s] The periodicity must be at least 1.", this.getClass().getName()));
			
		}
		
	}
}
