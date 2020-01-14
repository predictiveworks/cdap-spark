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

import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDFModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import de.kp.works.core.BaseFeatureCompute;
import de.kp.works.core.BaseFeatureConfig;
import de.kp.works.core.ml.SparkMLManager;
import de.kp.works.ml.MLUtils;
import de.kp.works.ml.feature.W2Vec.W2VecConfig;

@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("TFIDFProj")
@Description("A transformation stage that leverages an Apache Spark based TF-IDF model to map a sequence of words into "
		+ "its feature vector. Term frequency-inverse document frequency (TF-IDF) is a feature vectorization method widely "
		+ "used in text mining to reflect the importance of a term to a document in the corpus.")
public class TFIDFProj extends BaseFeatureCompute {

	private static final long serialVersionUID = 5252236917563666462L;

	private IDFModel model;
	private TFIDFManager manager;

	public TFIDFProj(TFIDFProjConfig config) {
		this.config = config;
		this.manager = new TFIDFManager();
	}

	@Override
	public void initialize(SparkExecutionPluginContext context) throws Exception {
		((W2VecConfig)config).validate();

		modelFs = SparkMLManager.getFeatureFS(context);
		modelMeta = SparkMLManager.getFeatureMeta(context);

		model = manager.read(modelFs, modelMeta, config.modelName);
		if (model == null)
			throw new IllegalArgumentException(String.format("[%s] A feature model with name '%s' does not exist.",
					this.getClass().getName(), config.modelName));

	}

	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {

		((W2VecConfig)config).validate();

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
	public void validateSchema(Schema inputSchema, BaseFeatureConfig config) {
		super.validateSchema(inputSchema, config);
		
		/** INPUT COLUMN **/
		isArrayOfString(config.inputCol);
		
	}
	
	@Override
	public Dataset<Row> compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {

		TFIDFProjConfig tfidfConfig = (TFIDFProjConfig)config;
		
		HashingTF transformer = new HashingTF();

		transformer.setInputCol(tfidfConfig.inputCol);
		transformer.setOutputCol("_features");
		/*
		 * Determine number of features from TF-IDF model
		 * metadata information
		 */
		Integer numFeatures = (Integer)manager.getParam(modelMeta, config.modelName, "numFeatures");
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
		return output;

	}

	/**
	 * A helper method to compute the output schema in that use cases where an input
	 * schema is explicitly given
	 */
	public Schema getOutputSchema(Schema inputSchema, String outputField) {

		List<Schema.Field> fields = new ArrayList<>(inputSchema.getFields());
		
		fields.add(Schema.Field.of(outputField, Schema.arrayOf(Schema.of(Schema.Type.DOUBLE))));
		return Schema.recordOf(inputSchema.getRecordName() + ".transformed", fields);

	}	
	
	public static class TFIDFProjConfig extends BaseFeatureConfig {

		private static final long serialVersionUID = 6981782808377365083L;
		
		public void validate() {
			super.validate();

		}
	}

}
