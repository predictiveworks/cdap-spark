package de.kp.works.ml.feature;
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

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import de.kp.works.core.FeatureConfig;
import de.kp.works.core.feature.FeatureCompute;

@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("QuantileDiscretizer")
@Description("A transformation stage that leverages an Apache Spark Quantile Discretizer to continuous input "
		+ "features onto binned categorical feature.")
public class QuantileDiscretizer extends FeatureCompute {
	/*
	 * 'QuantileDiscretizer' takes a column with continuous features and outputs a
	 * column with binned categorical features. The number of bins can be set using
	 * the `numBuckets` parameter. It is possible that the number of buckets used
	 * will be smaller than this value, for example, if there are too few distinct
	 * values of the input to create enough distinct quantiles.
	 *
	 * NaN handling: NaN values will be removed from the column during
	 * `QuantileDiscretizer` fitting. This will produce a `Bucketizer` model for
	 * making predictions. During the transformation, `Bucketizer` will raise an
	 * error when it finds NaN values in the dataset, but the user can also choose
	 * to either keep or remove NaN values within the dataset by setting
	 * `handleInvalid`. If the user chooses to keep NaN values, they will be handled
	 * specially and placed into their own bucket, for example, if 4 buckets are
	 * used, then non-NaN data will be put into buckets[0-3], but NaNs will be
	 * counted in a special bucket[4].
	 *
	 * Algorithm: The bin ranges are chosen using an approximate algorithm (see the
	 * documentation for
	 * `org.apache.spark.sql.DataFrameStatFunctions.approxQuantile` for a detailed
	 * description). The precision of the approximation can be controlled with the
	 * `relativeError` parameter. The lower and upper bin bounds will be `-Infinity`
	 * and `+Infinity`, covering all real values.
	 */
	private static final long serialVersionUID = -3391666113031818960L;

	public QuantileDiscretizer(QuantileDiscretizerConfig config) {
		this.config = config;
	}

	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {

		((QuantileDiscretizerConfig)config).validate();

		StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
		/*
		 * Try to determine input and output schema; if these schemas are not explicitly
		 * specified, they will be inferred from the provided data records
		 */
		inputSchema = stageConfigurer.getInputSchema();
		if (inputSchema != null) {

			validateSchema(inputSchema, config);
			/*
			 * In cases where the input schema is explicitly provided, we determine the
			 * output schema by explicitly adding the output column
			 */
			outputSchema = getOutputSchema(inputSchema, config.outputCol);
			stageConfigurer.setOutputSchema(outputSchema);

		}

	}
	
	@Override
	public void validateSchema(Schema inputSchema, FeatureConfig config) {
		super.validateSchema(inputSchema, config);
		
		/** INPUT COLUMN **/
		isNumeric(config.inputCol);
		
	}

	/**
	 * A helper method to compute the output schema in that use cases where an input
	 * schema is explicitly given
	 */
	public Schema getOutputSchema(Schema inputSchema, String outputField) {

		List<Schema.Field> fields = new ArrayList<>(inputSchema.getFields());
		
		fields.add(Schema.Field.of(outputField, Schema.of(Schema.Type.DOUBLE)));
		return Schema.recordOf(inputSchema.getRecordName() + ".transformed", fields);

	}	
	
	@Override
	public Dataset<Row> compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {

		/*
		 * Transformation from [Numeric] to [Double]
		 */
		QuantileDiscretizerConfig discretizerConfig = (QuantileDiscretizerConfig)config;
		
		org.apache.spark.ml.feature.QuantileDiscretizer transformer = new org.apache.spark.ml.feature.QuantileDiscretizer();
		transformer.setInputCol(discretizerConfig.inputCol);
		transformer.setOutputCol(discretizerConfig.outputCol);

		transformer.setNumBuckets(discretizerConfig.numBuckets);
		transformer.setRelativeError(discretizerConfig.relativeError);

		Dataset<Row> output = transformer.fit(source).transform(source);	
		return output;
	    		
	}

	public static class QuantileDiscretizerConfig extends FeatureConfig {

		private static final long serialVersionUID = 3497204830649437480L;

		@Description("The number of buckets (quantiles, or categories) into which data points are grouped. "
				+ "Must be greater than or equal to 2. Default is 2.")
		@Macro
		public Integer numBuckets;

		@Description("The relative target precision for the approximate quantile algorithm used to generate buckets. "
				+ "Must be in the range [0, 1]. Default is 0.001.")
		@Macro
		public Double relativeError;

		public QuantileDiscretizerConfig() {
			numBuckets = 2;
			relativeError = 0.001;
		}

		public void validate() {
			super.validate();

			if (numBuckets < 2) {
				throw new IllegalArgumentException(
						String.format("[%s] The number of buckets must be greater than 1.", this.getClass().getName()));
			}

			if (relativeError < 0 || relativeError > 1) {
				throw new IllegalArgumentException(String.format("[%s] The relative error must be in the range [0, 1].",
						this.getClass().getName()));
			}

		}
	}
}
