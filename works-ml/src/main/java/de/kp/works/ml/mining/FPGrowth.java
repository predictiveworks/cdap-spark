package de.kp.works.ml.mining;
/*
 * Copyright (c) 2019 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.SparkCompute;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;

import de.kp.works.core.BaseConfig;
import de.kp.works.core.SchemaUtil;
import de.kp.works.core.mining.MiningCompute;

@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("FPGrowth")
@Description("A transformation stage that leverages the Apache Spark ML FPGrowth algorithm to detect frequent patterns.")
public class FPGrowth extends MiningCompute {

	private static final long serialVersionUID = -6799602007838602218L;

	private final FPGrowthConfig config;
	
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

	@Override
	public void validateSchema(Schema inputSchema) {
		config.validateSchema(inputSchema);
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

		@Description("Minimal support level of the frequent pattern. Value must be in range [0.0, 1.0]. "
				+ "Any pattern that appears more than (minSupport * data size) times will be "
				+ "output in the frequent item sets. Default is 0.3.")
		@Macro
		public Double minSupport;

		@Description("Minimal confidence for generating Association Rule. minConfidence will not affect the mining "
				+ "for frequent item sets, but will affect the association rules generation. Default is 0.8.")
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
		
		public void validateSchema(Schema inputSchema) {

			/* ITEMS COLUMN */

			Schema.Field itemsField = inputSchema.getField(itemsCol);
			if (itemsField == null) {
				throw new IllegalArgumentException(String.format(
						"[%s] The input schema must contain the field that defines the items.", this.getClass().getName()));
			}
			
			/* ITEMS COLUMN */
			SchemaUtil.isArrayOfString(inputSchema, itemsCol);
			
		}
		
	}
}
