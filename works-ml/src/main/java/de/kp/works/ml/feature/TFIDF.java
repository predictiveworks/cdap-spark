package de.kp.works.ml.feature;
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

import de.kp.works.core.recording.feature.TFIDFRecorder;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDFModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.SparkCompute;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import de.kp.works.core.feature.FeatureConfig;
import de.kp.works.core.SchemaUtil;
import de.kp.works.core.feature.FeatureCompute;
import de.kp.works.core.recording.MLUtils;

@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("TFIDF")
@Description("A transformation stage that leverages an Apache Spark ML TF-IDF model to map a sequence of words into "
		+ "its feature vector. Term frequency-inverse document frequency (TF-IDF) is a feature vectorization method widely "
		+ "used in text mining to reflect the importance of a term to a document in the corpus.")
public class TFIDF extends FeatureCompute {

	private static final long serialVersionUID = 5252236917563666462L;

	private final TFIDFConfig config;
	
	private IDFModel model;
	private final TFIDFRecorder recorder;

	public TFIDF(TFIDFConfig config) {
		this.config = config;
		this.recorder = new TFIDFRecorder(configReader);
	}

	@Override
	public void initialize(SparkExecutionPluginContext context) throws Exception {
		config.validate();
		/*
		 * STEP #1: Retrieve the trained feature model that refers 
		 * to the provide name, stage and option. TFIDF models do 
		 * not have any metrics, i.e. there is no model option: 
		 * always the latest model is used
		 */
		model = recorder.read(context, config.modelName, config.modelStage, LATEST_MODEL);
		if (model == null)
			throw new IllegalArgumentException(String.format("[%s] A feature model with name '%s' does not exist.",
					this.getClass().getName(), config.modelName));

		/*
		 * STEP #2: Retrieve the profile of the trained feature 
		 * model for subsequent annotation
		 */
		profile = recorder.getProfile();

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
			outputSchema = getArrayOutputSchema(inputSchema, config.outputCol, Schema.Type.DOUBLE);
			stageConfigurer.setOutputSchema(outputSchema);

		}

	}
	
	@Override
	public void validateSchema(Schema inputSchema) {
		config.validateSchema(inputSchema);
	}
	
	@Override
	public Dataset<Row> compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {

		/*
		 * Transformation from Array[String] to Array[Double]
		 */
		HashingTF transformer = new HashingTF();

		transformer.setInputCol(config.inputCol);
		transformer.setOutputCol("_features");
		/*
		 * Determine number of features from TF-IDF model
		 * metadata information
		 */
		Integer numFeatures = (Integer)recorder.getParam(context, config.modelName, "numFeatures");
		transformer.setNumFeatures(numFeatures);

		Dataset<Row> transformedTF = transformer.transform(source);
		
		model.setInputCol("_features");
		/*
		 * The internal output of the TF-IDF model is an ML specific
		 * vector representation; this must be transformed into
		 * an Array[Double] to be compliant with Google CDAP
		 */		
		model.setOutputCol("_vector");		
		Dataset<Row> transformed = model.transform(transformedTF).drop("_features");		

		Dataset<Row> output = MLUtils.devectorize(transformed, "_vector", config.outputCol).drop("_vector");
		return annotate(output, FEATURE_TYPE);

	}
	
	public static class TFIDFConfig extends FeatureConfig {

		private static final long serialVersionUID = 6981782808377365083L;
		
		public void validate() {
			super.validate();

		}
		
		public void validateSchema(Schema inputSchema) {
			super.validateSchema(inputSchema);
			
			/* INPUT COLUMN */
			SchemaUtil.isArrayOfString(inputSchema, inputCol);
			
		}
	}

}
