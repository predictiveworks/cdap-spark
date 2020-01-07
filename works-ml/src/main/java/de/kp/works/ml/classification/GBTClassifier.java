package de.kp.works.ml.classification;
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

import java.util.HashMap;
import java.util.Map;

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
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import de.kp.works.core.BaseClassifierConfig;
import de.kp.works.core.BaseClassifierSink;

@Plugin(type = "sparksink")
@Name("GBTClassifer")
@Description("A building stage for an Apache Spark based Gradient-Boosted Trees classifier model.")
public class GBTClassifier extends BaseClassifierSink {

	private static final long serialVersionUID = 2970318408059323693L;

	private GBTClassifierConfig config;
	
	public GBTClassifier(GBTClassifierConfig config) {
		this.config = config;
	}

	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
		super.configurePipeline(pipelineConfigurer);

		/* Validate configuration */
		config.validate();
		
		/* Validate schema */
		StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
		Schema inputSchema = stageConfigurer.getInputSchema();

		validateSchema(inputSchema, config, GBTClassifier.class.getName());

	}
	
	@Override
	public void compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {
		/*
		 * STEP #1: Extract parameters and train classifier model
		 */
		String featuresCol = config.featuresCol;
		Map<String, Object> params = config.getParamsAsMap();
		/*
		 * The vectorCol specifies the internal column that has
		 * to be built from the featuresCol and that is used for
		 * training purposes
		 */
		String vectorCol = "_vector";
		/*
		 * Prepare provided dataset by vectorizing the feature
		 * column which is specified as Array[Double]
		 */
		GBTTrainer trainer = new GBTTrainer();
		Dataset<Row> vectorset = trainer.vectorize(source, featuresCol, vectorCol);
		
		// TODO

	}

	public static class GBTClassifierConfig extends BaseClassifierConfig {

		private static final long serialVersionUID = 7335333448330182611L;

		@Description("The type of the loss function the Gradient-Boosted Trees algorithm tries to minimize. Default is 'logistic'.")
		@Macro
		public String lossType;
		
		@Description("The maximum number of bins used for discretizing continuous features and for choosing how to split "
				+ " on features at each node. More bins give higher granularity. Must be at least 2. Default is 32.")
		@Macro
		public Integer maxBins;
		
		@Description("Nonnegative value that maximum depth of the tree. E.g. depth 0 means 1 leaf node; "
				+" depth 1 means 1 internal node + 2 leaf nodes. Default is 5.")
		@Macro
		public Integer maxDepth;

		@Description("The maximum number of iterations to train the Gradient-Boosted Trees model. Default is 20.")
		@Macro
		public Integer maxIter;

		@Description("The minimum information gain for a split to be considered at a tree node. The value should be at least 0.0. Default is 0.0.")
		@Macro
		public Double minInfoGain;
		
		@Description("The learning rate for shrinking the contribution of each estimator. Must be in interval (0, 1]. Default is 0.1")
		@Macro
		public Double stepSize;
		
		public GBTClassifierConfig() {

			dataSplit = "70:30";
			
			lossType = "logistic";
			minInfoGain = 0D;

			maxBins = 32;
			maxDepth = 5;
			
			maxIter = 20;
			stepSize = 0.1;

		}
	    
		@Override
		public Map<String, Object> getParamsAsMap() {
			
			Map<String, Object> params = new HashMap<>();
			params.put("minInfoGain", minInfoGain);
			
			params.put("maxBins", maxBins);
			params.put("maxDepth", maxDepth);

			params.put("maxIter", maxIter);
			params.put("stepSize", stepSize);

			params.put("split", dataSplit);
			return params;
		
		}

		public void validate() {

			/** MODEL & COLUMNS **/
			if (!Strings.isNullOrEmpty(modelName)) {
				throw new IllegalArgumentException("[GBTClassifierConfig] The model name must not be empty.");
			}
			if (!Strings.isNullOrEmpty(featuresCol)) {
				throw new IllegalArgumentException("[GBTClassifierConfig] The name of the field that contains the feature vector must not be empty.");
			}
			if (!Strings.isNullOrEmpty(labelCol)) {
				throw new IllegalArgumentException("[GBTClassifierConfig] The name of the field that contains the label value must not be empty.");
			}
			
			/** PARAMETERS **/
			if (maxBins < 2)
				throw new IllegalArgumentException("[GBTClassifierConfig] The maximum bins must be at least 2.");

			if (maxDepth < 0)
				throw new IllegalArgumentException("[GBTClassifierConfig] The maximum depth must be nonnegative.");
			
			if (minInfoGain < 0D)
				throw new IllegalArgumentException("[GBTClassifierConfig] The minimum information gain must be at least 0.0.");
			
			if (maxIter < 1)
				throw new IllegalArgumentException("[GBTClassifierConfig] The maximum number of iterations must be at least 1.");
						
			if (stepSize <= 0D || stepSize > 1D)
				throw new IllegalArgumentException("[GBTClassifierConfig] The learning rate must be in interval (0, 1].");

		}
		
	}
}
