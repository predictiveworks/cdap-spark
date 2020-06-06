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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.SparkCompute;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;

import de.kp.works.core.time.TimeCompute;
import de.kp.works.core.time.TimeConfig;
import de.kp.works.core.ml.MLUtils;

@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("TsVectorize")
@Description("A time series transformation stage that vectorizes time series data "
		+ "by assembling each observation in time and its past N-1 observations into "
		+ "a feature vector.")
public class TsVectorize extends TimeCompute {

	private static final long serialVersionUID = -3722021453251234803L;

	private TsVectorizeConfig config;
	
	public TsVectorize(TsVectorizeConfig config) {
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

			outputSchema = getOutputSchema(inputSchema);
			stageConfigurer.setOutputSchema(outputSchema);

		}

	}
	
	@Override
	public Dataset<Row> compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {

		Lagging lagging = new Lagging();
		
		lagging.setLag(config.vectorSize);
		lagging.setLaggingType("features");
		
		lagging.setFeaturesCol(config.featuresCol);
		/*
		 * The feature vector of the transformation stage is 
		 * specified by Apache Spark ML vector format has to be
		 * transformed into an array of double to be compliant
		 * with Google CDAP
		 */
		Dataset<Row> output = MLUtils.devectorize(lagging.transform(source), config.featuresCol, config.featuresCol);		
		return output;
		
	}

	public Schema getOutputSchema(Schema inputSchema) {
		
		List<Schema.Field> outfields = new ArrayList<>();
		for (Schema.Field field: inputSchema.getFields()) {
			/*
			 * Cast the data type of the value field to double
			 */
			if (field.getName().equals(config.valueCol)) {
				outfields.add(Schema.Field.of(config.valueCol, Schema.of(Schema.Type.DOUBLE)));
				
			} else
				outfields.add(field);
		}
		
		outfields.add(Schema.Field.of(config.featuresCol, Schema.arrayOf(Schema.of(Schema.Type.DOUBLE))));
		
		return Schema.recordOf(inputSchema.getRecordName() + ".transformed", outfields);

	}

	@Override
	public void validateSchema(Schema inputSchema) {
		config.validateSchema(inputSchema);
	}

	public static class TsVectorizeConfig extends TimeConfig {

		private static final long serialVersionUID = 6119699429989993083L;

		@Description("The name of the field in the output schema that contains the feature vector.")
		@Macro
		public String featuresCol;

		@Description("The dimension of the feature vector, i.e the number of observations in time that are assembled as a vector. Default is 10.")
		@Macro
		public Integer vectorSize;

		public TsVectorizeConfig() {
			vectorSize = 10;
		}

		public void validate() {
			super.validate();

			if (Strings.isNullOrEmpty(featuresCol)) {
				throw new IllegalArgumentException(
						String.format("[%s] The name of the field that contains the feature vector must not be empty.", this.getClass().getName()));
			}

			if (vectorSize < 1) {
				throw new IllegalArgumentException(
						String.format("[%s] The size of the feature vector must be greater than 0.", this.getClass().getName()));
			}

		}
		
	}
}
