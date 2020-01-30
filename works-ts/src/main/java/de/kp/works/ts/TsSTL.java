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
import de.kp.works.core.TimeCompute;
import de.kp.works.core.TimeConfig;

@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("TsDecompose")
@Description("A time series transformation stage to decompose each time signal into seasonality, "
		+ "trend and remainder component leveraging an STL algorithm (Seasonal and Trend decomposition using Loess). "
		+ "This transformation stage adds 'seasonal', 'trend' and 'remainder' fields to the each time record.")
public class TsSTL extends TimeCompute {

	private static final long serialVersionUID = -8650664753408204785L;
	/*
	 * STL is a filtering procedure for decomposing a time series into trend, seasonal and 
	 * remainder components and consists of a sequence of applications of the LOESS smoother.
	 * 
	 */
	private TsDecomposeConfig config;

	public TsSTL(TsDecomposeConfig config) {
		this.config = config;
	}
	
	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {

		((TsDecomposeConfig)config).validate();

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
	
	/**
	 * A helper method to compute the output schema in that use cases where an input
	 * schema is explicitly given
	 */
	@Override
	public Schema getOutputSchema(Schema inputSchema) {
		
		List<Schema.Field> outfields = new ArrayList<>();
		for (Schema.Field field: inputSchema.getFields()) {
			/*
			 * Cast value field into Double field
			 */
			if (field.getName().equals(config.valueCol)) {
				outfields.add(Schema.Field.of(config.valueCol, Schema.of(Schema.Type.DOUBLE)));
				
			} else
				outfields.add(field);
		}
		/*
		 * Add STL specific output fields to the output schema
		 */
		outfields.add(Schema.Field.of("seasonal", Schema.of(Schema.Type.DOUBLE)));
		outfields.add(Schema.Field.of("trend", Schema.of(Schema.Type.DOUBLE)));
		outfields.add(Schema.Field.of("remainder", Schema.of(Schema.Type.DOUBLE)));

		return Schema.recordOf(inputSchema.getRecordName() + ".decomposed", outfields);

	}
	@Override
	public Dataset<Row> compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {

		TsDecomposeConfig computeConfig = (TsDecomposeConfig)config;
		
		STL decomposer = new STL();
		
		/** COLUMNS **/
		decomposer.setTimeCol(computeConfig.timeCol);
		decomposer.setValueCol(computeConfig.valueCol);
		decomposer.setGroupCol(computeConfig.groupCol);
		
		/** PARAMETERS **/
		decomposer.setOuterIter(computeConfig.outerIter);
		decomposer.setInnerIter(computeConfig.innerIter);
		
		decomposer.setPeriodicity(computeConfig.periodicity);

		decomposer.setSeasonalLoessSize(computeConfig.seasonalLoessSize);
		decomposer.setTrendLoessSize(computeConfig.trendLoessSize);
		
		decomposer.setLevelLoessSize(computeConfig.levelLoessSize);
		
		Dataset<Row> output = decomposer.transform(source);
		return output;
	
	}
	
	public static class TsDecomposeConfig extends TimeConfig {

		private static final long serialVersionUID = 7484792664574626489L;

		@Description("The name of the field in the input schema that contains the group value required by "
				+ "decomposition algorithm.")
		@Macro
		public String groupCol;

		@Description("The positive number of cycles through the outer loop. More cycles here reduce the affect of outliers. "
				+ " For most situations this can be quite small. Default is 1.")
		@Macro
		public Integer outerIter;
		
		@Description("The positive number of cycles through the inner loop. Number of cycles should be large enough to reach "
				+ "convergence,  which is typically only two or three. When multiple outer cycles, the number of inner cycles "
				+ "can be smaller as they do not necessarily help get overall convergence. Default value is 2.")
		@Macro
		public Integer innerIter;
		
		@Description("The periodicity of the seasonality; should be equal to lag of the autocorrelation function with the "
				+ "highest (positive) correlation.")
		@Macro
		public Integer periodicity;
		
		@Description("The length of the seasonal LOESS smoother.")
		@Macro
		public Integer seasonalLoessSize;
		
		@Description("The length of the trend LOESS smoother.")
		@Macro
		public Integer trendLoessSize;
		
		@Description("The length of the level LOESS smoother.")
		@Macro
		public Integer levelLoessSize;
		
		public void validate() {
			super.validate();

			if (Strings.isNullOrEmpty(groupCol))
				throw new IllegalArgumentException(
						String.format("[%s] The name of the field that is used for grouping must not be empty.", this.getClass().getName()));
			
			if (outerIter < 1)
				throw new IllegalArgumentException(String.format(
						"[%s] The number of outer cycles must be at least 1.", this.getClass().getName()));
			
			if (innerIter < 1)
				throw new IllegalArgumentException(String.format(
						"[%s] The number of inner cycles must be at least 1.", this.getClass().getName()));
			
			if (periodicity < 1)
				throw new IllegalArgumentException(String.format(
						"[%s] The periodicity must be at least 1.", this.getClass().getName()));

			if (seasonalLoessSize < 1)
				throw new IllegalArgumentException(String.format(
						"[%s] The size of the seasonal smoother must be at least 1.", this.getClass().getName()));

			if (trendLoessSize < 1)
				throw new IllegalArgumentException(String.format(
						"[%s] The size of the trend smoother must be at least 1.", this.getClass().getName()));

			if (levelLoessSize < 1)
				throw new IllegalArgumentException(String.format(
						"[%s] The size of the level smoother must be at least 1.", this.getClass().getName()));
			
		}
		
	}
}
