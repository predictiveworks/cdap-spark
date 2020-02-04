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
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import de.kp.works.core.ClassifierConfig;
import de.kp.works.core.ClassifierSink;

@Plugin(type = "sparksink")
@Name("OVRClassifer")
@Description("A building stage for an Apache Spark based OneVsRest classifier model.")
public class OVRClassifier extends ClassifierSink {

	private static final long serialVersionUID = 3428241323322243511L;
	
	public OVRClassifier(OVRClassifierConfig config) {
		this.config = config;
		this.className = OVRClassifier.class.getName();
	}

	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
		super.configurePipeline(pipelineConfigurer);

		/* Validate configuration */
		((OVRClassifierConfig)config).validate();
		
		/* Validate schema */
		StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
		inputSchema = stageConfigurer.getInputSchema();
		if (inputSchema != null)
			validateSchema(inputSchema, config);

	}
	
	@SuppressWarnings("unused")
	@Override
	public void compute(SparkExecutionPluginContext context, Dataset<Row> source) throws Exception {
		
		OVRClassifierConfig classifierConfig = (OVRClassifierConfig)config;
		/*
		 * STEP #1: Extract parameters and train classifier model
		 */
		String featuresCol = classifierConfig.featuresCol;
		Map<String, Object> params = classifierConfig.getParamsAsMap();
		/*
		 * The vectorCol specifies the internal column that has
		 * to be built from the featuresCol and that is used for
		 * training purposes
		 */
		String vectorCol = "_vector";
		/*
		 * Prepare provided dataset by vectorizing the feature
		 * column which is specified as Array[Numeric]
		 */
		OVRTrainer trainer = new OVRTrainer();
		Dataset<Row> vectorset = trainer.vectorize(source, featuresCol, vectorCol);
		
		// TODO

	}

	public static class OVRClassifierConfig extends ClassifierConfig {

		private static final long serialVersionUID = 1504291913928740352L;
		
		public OVRClassifierConfig() {
			/*
			 * The default split of the dataset into train & test data
			 * is set to 70:30
			 */
			dataSplit = "70:30";			
			
		}

		@Override
		public Map<String, Object> getParamsAsMap() {
			
			Map<String, Object> params = new HashMap<>();
			params.put("split", dataSplit);

			return params;
		
		}

		public void validate() {

			/** MODEL & COLUMNS **/
			if (Strings.isNullOrEmpty(modelName)) {
				throw new IllegalArgumentException("[OVRClassifierConfig] The model name must not be empty.");
			}
			if (Strings.isNullOrEmpty(featuresCol)) {
				throw new IllegalArgumentException("[OVRClassifierConfig] The name of the field that contains the feature vector must not be empty.");
			}
			
			// TODO validate parameters
						
		}
		
	}
}
