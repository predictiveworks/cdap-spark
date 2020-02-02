package de.kp.works.ml.mining;

import java.util.ArrayList;
import java.util.List;
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
import de.kp.works.core.BaseCompute;
import de.kp.works.core.BaseConfig;

@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("FPGrowth")
@Description("A transformation stage that leverages the Apache Spark FPGrowth algorithm to detect frequent patterns.")
public class FPGrowth extends BaseCompute {

	private static final long serialVersionUID = -6799602007838602218L;

	private FPGrowthConfig config;
	
	public FPGrowth(FPGrowthConfig config) {
		this.config = config;
	}

	@Override
	public void initialize(SparkExecutionPluginContext context) throws Exception {
		config.validate();

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
			 * output schema by explicitly adding the output column
			 */
			outputSchema = getOutputSchema(inputSchema);
			stageConfigurer.setOutputSchema(outputSchema);

		}

	}
	
	@Override
	public Dataset<Row> compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {

		org.apache.spark.ml.fpm.FPGrowth miner = new org.apache.spark.ml.fpm.FPGrowth();
		miner.setItemsCol(config.itemsCol);
		
		miner.setMinSupport(config.minSupport);
		miner.setMinConfidence(config.minConfidence);
		
		return miner.fit(source).transform(source);
		
	}
	
	public void validateSchema(Schema inputSchema) {

		/** ITEMS COLUMN **/

		Schema.Field itemsCol = inputSchema.getField(config.itemsCol);
		if (itemsCol == null) {
			throw new IllegalArgumentException(String.format(
					"[%s] The input schema must contain the field that defines the items.", this.getClass().getName()));
		}
		
		/** ITEMS COLUMN **/
		isArrayOfString(config.itemsCol);
		
	}

	/**
	 * A helper method to compute the output schema in that use cases where an input
	 * schema is explicitly given
	 */
	public Schema getOutputSchema(Schema inputSchema) {

		List<Schema.Field> fields = new ArrayList<>();
		
		fields.add(Schema.Field.of("antecedent", Schema.of(Schema.Type.STRING)));
		fields.add(Schema.Field.of("consequent", Schema.of(Schema.Type.STRING)));
		
		fields.add(Schema.Field.of("confidence", Schema.of(Schema.Type.DOUBLE)));
		return Schema.recordOf(inputSchema.getRecordName() + ".patterns", fields);

	}

	public static class FPGrowthConfig extends BaseConfig {

		private static final long serialVersionUID = -8056587602204640221L;

		@Description("The name of the field in the into schema that contains the items.")
		@Macro
		public String itemsCol;

		/**
		 * Minimal support level of the frequent pattern. [0.0, 1.0]. Any pattern that
		 * appears more than (minSupport * size-of-the-dataset) times will be output in
		 * the frequent itemsets. Default: 0.3
		 */

		@Description("Minimal support level of the frequent pattern. Value must be in range [0.0, 1.0]. "
				+ "Any pattern that appears * more than (minSupport * size-of-the-dataset) times will be "
				+ "output in the frequent itemsets. Default is 0.3.")
		@Macro
		public Double minSupport;

		@Description("Minimal confidence for generating Association Rule. minConfidence will not affect the mining "
				+ "	* for frequent itemsets, but will affect the association rules generation. Default is 0.8.")
		@Macro
		public Double minConfidence;

		public FPGrowthConfig() {
			minSupport = 0.3;
			minConfidence = 0.8;
		}
		
		public void validate() {
			super.validate();
			
			if (Strings.isNullOrEmpty(itemsCol)) {
				throw new IllegalArgumentException(String.format(
						"[%s] The name of the field that contains the items must not be empty.",
						this.getClass().getName()));
			}

			if (minSupport < 0 || minSupport > 1) {
				throw new IllegalArgumentException(String.format(
						"[%s] The The minimal support must be in range [0.0, 1.0].",
						this.getClass().getName()));
			}

			if (minConfidence < 0 || minConfidence > 1) {
				throw new IllegalArgumentException(String.format(
						"[%s] The The minimal support must be in range [0.0, 1.0].",
						this.getClass().getName()));
			}
			
		}
	}
}
